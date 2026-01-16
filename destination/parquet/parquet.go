package parquet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	pqgo "github.com/parquet-go/parquet-go"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
)

type FileMetadata struct {
	fileName string
	writer   any
	file     source.ParquetFile
}

// Parquet destination writes Parquet files to a local path and optionally uploads them to S3.
type Parquet struct {
	options          *destination.Options
	config           *Config
	stream           types.StreamInterface
	basePath         string                   // construct with streamNamespace/streamName
	partitionedFiles map[string]*FileMetadata // mapping of basePath/{regex} -> pqFiles
	s3Client         *s3.S3
	s3Uploader       *s3manager.Uploader
	schema           typeutils.Fields

	// Cumulative benchmark metrics
	totalBatchDuration     time.Duration
	totalPerRecordDuration time.Duration
	totalBatchMemDelta     uint64
	totalPerRecordMemDelta uint64
	totalRecordsSynced     uint64
}

// GetConfigRef returns the config reference for the parquet writer.
func (p *Parquet) GetConfigRef() destination.Config {
	p.config = &Config{}
	return p.config
}

// Spec returns a new Config instance.
func (p *Parquet) Spec() any {
	return Config{}
}

// setup s3 client if credentials provided
func (p *Parquet) initS3Writer() error {
	if p.config.Bucket == "" || p.config.Region == "" {
		return nil
	}

	s3Config := aws.Config{
		Region: aws.String(p.config.Region),
	}
	if p.config.S3Endpoint != "" {
		s3Config.Endpoint = aws.String(p.config.S3Endpoint)
		// Force path-style URLs (e.g., http://minio:9000/bucket/key) to support MinIO and avoid bucket-based DNS resolution
		s3Config.S3ForcePathStyle = aws.Bool(true)
	}
	if p.config.AccessKey != "" && p.config.SecretKey != "" {
		s3Config.Credentials = credentials.NewStaticCredentials(p.config.AccessKey, p.config.SecretKey, "")
	}
	sess, err := session.NewSession(&s3Config)
	if err != nil {
		return fmt.Errorf("failed to create AWS session: %s", err)
	}
	p.s3Client = s3.New(sess)
	// Initialize uploader for multipart uploads (handles files > 5GB automatically)
	p.s3Uploader = s3manager.NewUploader(sess)

	return nil
}

func (p *Parquet) createNewPartitionFile(basePath string) error {
	// construct directory path
	directoryPath := filepath.Join(p.config.Path, basePath)

	if err := os.MkdirAll(directoryPath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories[%s]: %s", directoryPath, err)
	}

	fileName := utils.TimestampedFileName(constants.ParquetFileExt)
	filePath := filepath.Join(directoryPath, fileName)

	pqFile, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return fmt.Errorf("failed to create parquet file writer: %s", err)
	}

	writer := func() any {
		if p.stream.NormalizationEnabled() {
			return pqgo.NewGenericWriter[any](pqFile, p.schema.ToTypeSchema().ToParquet(), pqgo.Compression(&pqgo.Snappy))
		}
		return pqgo.NewGenericWriter[types.RawRecord](pqFile, types.GetParquetRawSchema(), pqgo.Compression(&pqgo.Snappy))
	}()

	p.partitionedFiles[basePath] = &FileMetadata{
		fileName: fileName,
		file:     pqFile,
		writer:   writer,
	}

	logger.Infof("Thread[%s]: created new partition file[%s]", p.options.ThreadID, filePath)
	return nil
}

// Setup configures the parquet writer, including local paths, file names, and optional S3 setup.
func (p *Parquet) Setup(_ context.Context, stream types.StreamInterface, schema any, options *destination.Options) (any, error) {
	p.options = options
	p.stream = stream
	p.partitionedFiles = make(map[string]*FileMetadata)
	p.basePath = filepath.Join(p.stream.GetDestinationDatabase(nil), p.stream.GetDestinationTable())
	p.schema = make(typeutils.Fields)

	// Reset cumulative benchmark metrics
	p.totalBatchDuration = 0
	p.totalPerRecordDuration = 0
	p.totalBatchMemDelta = 0
	p.totalPerRecordMemDelta = 0
	p.totalRecordsSynced = 0

	// for s3 p.config.path may not be provided
	if p.config.Path == "" {
		p.config.Path = os.TempDir()
	}

	err := p.initS3Writer()
	if err != nil {
		return nil, err
	}

	if !p.stream.NormalizationEnabled() {
		return p.schema, nil
	}

	if schema != nil {
		fields, ok := schema.(typeutils.Fields)
		if !ok {
			return nil, fmt.Errorf("failed to typecast schema[%T] into typeutils.Fields", schema)
		}
		p.schema = fields.Clone()
		return fields, nil
	}

	fields := make(typeutils.Fields)
	fields.FromSchema(stream.Schema())
	p.schema = fields.Clone() // update schema
	return fields, nil
}

// Write writes a record to the Parquet file.
// This function runs BOTH batch and per-record writes for benchmarking comparison
// Write writes a record to the Parquet file.
// This function runs EITHER batch OR per-record writes based on the toggle
func (p *Parquet) Write(ctx context.Context, records []types.RawRecord) error {
	// TOGGLE: Set to true for batch writing, false for per-record writing
	const useBatchWriting = true

	p.totalRecordsSynced += uint64(len(records))

	startTime := time.Now()
	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	var err error
	if useBatchWriting {
		err = p.writeBatch(ctx, records)
	} else {
		err = p.writePerRecord(ctx, records)
	}

	if err != nil {
		logger.Warnf("Thread[%s]: Write operation failed: %s", p.options.ThreadID, err)
		return err
	}

	runtime.ReadMemStats(&memAfter)
	duration := time.Since(startTime)
	memDelta := memAfter.TotalAlloc - memBefore.TotalAlloc

	// Accumulate metrics for the active mode
	if useBatchWriting {
		p.totalBatchDuration += duration
		p.totalBatchMemDelta += memDelta
	} else {
		p.totalPerRecordDuration += duration
		p.totalPerRecordMemDelta += memDelta
	}

	return nil
}

// writeBatch writes records using batch buffering (plain version without optimizations)
func (p *Parquet) writeBatch(_ context.Context, records []types.RawRecord) error {
	const batchSize = 5000

	// Debug: Start timing and capture initial state
	writeStartTime := time.Now()
	totalRecords := len(records)
	var memStatsBefore, memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)

	logger.Debugf("Thread[%s]: [BATCH_WRITE_START] Starting BATCH write operation - total_records=%d, batch_size=%d, normalization_enabled=%v",
		p.options.ThreadID, totalRecords, batchSize, p.stream.NormalizationEnabled())

	// Debug: Track flush statistics
	var normalizedFlushCount, rawFlushCount int
	var totalNormalizedFlushed, totalRawFlushed int
	var totalFlushDuration time.Duration

	// Local buffers for each partition path (plain approach)
	normalizedBuffers := make(map[string][]any)
	rawBuffers := make(map[string][]types.RawRecord)
	normalizationEnabled := p.stream.NormalizationEnabled()

	// Flush function for normalized records
	flushNormalized := func(partitionedPath string, buffer []any) error {
		flushStart := time.Now()
		partitionFile := p.partitionedFiles[partitionedPath]
		if partitionFile == nil {
			return fmt.Errorf("failed to find partition file for path[%s]", partitionedPath)
		}
		writtenRows, err := partitionFile.writer.(*pqgo.GenericWriter[any]).Write(buffer)
		flushDuration := time.Since(flushStart)
		totalFlushDuration += flushDuration

		if err != nil {
			logger.Debugf("Thread[%s]: [BATCH_FLUSH_ERROR] Normalized flush failed - partition=%s, buffer_size=%d, error=%s",
				p.options.ThreadID, partitionedPath, len(buffer), err)
			return fmt.Errorf("failed to write batch in parquet file: %s", err)
		}

		normalizedFlushCount++
		totalNormalizedFlushed += writtenRows
		logger.Debugf("Thread[%s]: [BATCH_FLUSH_NORMALIZED] partition=%s, records_flushed=%d, flush_duration=%v, flush_count=%d",
			p.options.ThreadID, partitionedPath, writtenRows, flushDuration, normalizedFlushCount)
		return nil
	}

	// Flush function for raw records
	flushRaw := func(partitionedPath string, buffer []types.RawRecord) error {
		flushStart := time.Now()
		partitionFile := p.partitionedFiles[partitionedPath]
		if partitionFile == nil {
			return fmt.Errorf("failed to find partition file for path[%s]", partitionedPath)
		}
		writtenRows, err := partitionFile.writer.(*pqgo.GenericWriter[types.RawRecord]).Write(buffer)
		flushDuration := time.Since(flushStart)
		totalFlushDuration += flushDuration

		if err != nil {
			logger.Debugf("Thread[%s]: [BATCH_FLUSH_ERROR] Raw flush failed - partition=%s, buffer_size=%d, error=%s",
				p.options.ThreadID, partitionedPath, len(buffer), err)
			return fmt.Errorf("failed to write batch in parquet file: %s", err)
		}

		rawFlushCount++
		totalRawFlushed += writtenRows
		logger.Debugf("Thread[%s]: [BATCH_FLUSH_RAW] partition=%s, records_flushed=%d, flush_duration=%v, flush_count=%d",
			p.options.ThreadID, partitionedPath, writtenRows, flushDuration, rawFlushCount)
		return nil
	}

	// Debug: Track partition creation
	partitionCreationStart := time.Now()
	newPartitionsCreated := 0

	// Process all records and accumulate in buffers
	for _, record := range records {
		record.OlakeTimestamp = time.Now().UTC()
		partitionedPath := p.getPartitionedFilePath(record.Data, record.OlakeTimestamp)
		partitionFile, exists := p.partitionedFiles[partitionedPath]
		if !exists {
			partitionStart := time.Now()
			err := p.createNewPartitionFile(partitionedPath)
			if err != nil {
				return fmt.Errorf("failed to create partition file: %s", err)
			}
			newPartitionsCreated++
			logger.Debugf("Thread[%s]: [PARTITION_CREATED] partition=%s, creation_time=%v, total_partitions=%d",
				p.options.ThreadID, partitionedPath, time.Since(partitionStart), len(p.partitionedFiles))
			partitionFile = p.partitionedFiles[partitionedPath]
		}

		if partitionFile == nil {
			return fmt.Errorf("failed to create partition file for path[%s]", partitionedPath)
		}

		// Append to appropriate buffer based on normalization setting
		if normalizationEnabled {
			normalizedBuffers[partitionedPath] = append(normalizedBuffers[partitionedPath], record.Data)
			// Flush when buffer reaches batch size
			if len(normalizedBuffers[partitionedPath]) >= batchSize {
				if err := flushNormalized(partitionedPath, normalizedBuffers[partitionedPath]); err != nil {
					return err
				}
				normalizedBuffers[partitionedPath] = normalizedBuffers[partitionedPath][:0] // reset buffer
			}
		} else {
			rawBuffers[partitionedPath] = append(rawBuffers[partitionedPath], record)
			// Flush when buffer reaches batch size
			if len(rawBuffers[partitionedPath]) >= batchSize {
				if err := flushRaw(partitionedPath, rawBuffers[partitionedPath]); err != nil {
					return err
				}
				rawBuffers[partitionedPath] = rawBuffers[partitionedPath][:0] // reset buffer
			}
		}
	}

	partitionCreationDuration := time.Since(partitionCreationStart)

	// Flush any remaining records in buffers
	finalFlushStart := time.Now()
	if normalizationEnabled {
		for partitionedPath, buffer := range normalizedBuffers {
			if len(buffer) > 0 {
				if err := flushNormalized(partitionedPath, buffer); err != nil {
					return err
				}
			}
		}
	} else {
		for partitionedPath, buffer := range rawBuffers {
			if len(buffer) > 0 {
				if err := flushRaw(partitionedPath, buffer); err != nil {
					return err
				}
			}
		}
	}
	finalFlushDuration := time.Since(finalFlushStart)

	// Debug: Capture final memory stats and calculate metrics
	runtime.ReadMemStats(&memStatsAfter)
	totalWriteDuration := time.Since(writeStartTime)

	// Calculate throughput metrics
	var recordsPerSecond float64
	if totalWriteDuration.Seconds() > 0 {
		recordsPerSecond = float64(totalRecords) / totalWriteDuration.Seconds()
	}

	// Memory delta
	allocDelta := memStatsAfter.Alloc - memStatsBefore.Alloc
	totalAllocDelta := memStatsAfter.TotalAlloc - memStatsBefore.TotalAlloc

	// Log comprehensive benchmark summary
	logger.Debugf("Thread[%s]: [BATCH_WRITE_COMPLETE] "+
		"mode=BATCH, total_records=%d, total_duration=%v, records_per_second=%.2f, "+
		"normalized_flushes=%d, raw_flushes=%d, total_normalized_flushed=%d, total_raw_flushed=%d, "+
		"total_flush_duration=%v, final_flush_duration=%v, "+
		"new_partitions_created=%d, partition_processing_duration=%v, total_partitions=%d, "+
		"memory_alloc_delta_bytes=%d, memory_total_alloc_delta_bytes=%d",
		p.options.ThreadID,
		totalRecords, totalWriteDuration, recordsPerSecond,
		normalizedFlushCount, rawFlushCount, totalNormalizedFlushed, totalRawFlushed,
		totalFlushDuration, finalFlushDuration,
		newPartitionsCreated, partitionCreationDuration, len(p.partitionedFiles),
		allocDelta, totalAllocDelta)

	return nil
}

// writePerRecord writes records one at a time (original non-batch approach)
func (p *Parquet) writePerRecord(_ context.Context, records []types.RawRecord) error {
	// Debug: Start timing and capture initial state
	writeStartTime := time.Now()
	totalRecords := len(records)
	var memStatsBefore, memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)

	logger.Debugf("Thread[%s]: [PER_RECORD_WRITE_START] Starting PER-RECORD write operation - total_records=%d, normalization_enabled=%v",
		p.options.ThreadID, totalRecords, p.stream.NormalizationEnabled())

	// Debug: Track write statistics
	var normalizedWriteCount, rawWriteCount int
	var totalWriteDurationAccum time.Duration
	normalizationEnabled := p.stream.NormalizationEnabled()

	// Debug: Track partition creation
	partitionCreationStart := time.Now()
	newPartitionsCreated := 0

	for idx, record := range records {
		record.OlakeTimestamp = time.Now().UTC()
		partitionedPath := p.getPartitionedFilePath(record.Data, record.OlakeTimestamp)
		partitionFile, exists := p.partitionedFiles[partitionedPath]
		if !exists {
			partitionStart := time.Now()
			err := p.createNewPartitionFile(partitionedPath)
			if err != nil {
				return fmt.Errorf("failed to create partition file: %s", err)
			}
			newPartitionsCreated++
			logger.Debugf("Thread[%s]: [PARTITION_CREATED] partition=%s, creation_time=%v, total_partitions=%d",
				p.options.ThreadID, partitionedPath, time.Since(partitionStart), len(p.partitionedFiles))
			partitionFile = p.partitionedFiles[partitionedPath]
		}

		if partitionFile == nil {
			return fmt.Errorf("failed to create partition file for path[%s]", partitionedPath)
		}

		// Write single record
		writeStart := time.Now()
		if normalizationEnabled {
			_, err := partitionFile.writer.(*pqgo.GenericWriter[any]).Write([]any{record.Data})
			if err != nil {
				logger.Debugf("Thread[%s]: [PER_RECORD_WRITE_ERROR] Normalized write failed - record_idx=%d, partition=%s, error=%s",
					p.options.ThreadID, idx, partitionedPath, err)
				return fmt.Errorf("failed to write in parquet file: %s", err)
			}
			normalizedWriteCount++
		} else {
			_, err := partitionFile.writer.(*pqgo.GenericWriter[types.RawRecord]).Write([]types.RawRecord{record})
			if err != nil {
				logger.Debugf("Thread[%s]: [PER_RECORD_WRITE_ERROR] Raw write failed - record_idx=%d, partition=%s, error=%s",
					p.options.ThreadID, idx, partitionedPath, err)
				return fmt.Errorf("failed to write in parquet file: %s", err)
			}
			rawWriteCount++
		}
		writeDuration := time.Since(writeStart)
		totalWriteDurationAccum += writeDuration

		// Log every 1000 records for progress tracking
		if (idx+1)%1000 == 0 {
			logger.Debugf("Thread[%s]: [PER_RECORD_PROGRESS] records_written=%d/%d, avg_write_time=%v",
				p.options.ThreadID, idx+1, totalRecords, totalWriteDurationAccum/time.Duration(idx+1))
		}
	}

	partitionCreationDuration := time.Since(partitionCreationStart)

	// Debug: Capture final memory stats and calculate metrics
	runtime.ReadMemStats(&memStatsAfter)
	totalWriteDuration := time.Since(writeStartTime)

	// Calculate throughput metrics
	var recordsPerSecond float64
	if totalWriteDuration.Seconds() > 0 {
		recordsPerSecond = float64(totalRecords) / totalWriteDuration.Seconds()
	}

	// Calculate average write time per record
	var avgWriteTime time.Duration
	if totalRecords > 0 {
		avgWriteTime = totalWriteDurationAccum / time.Duration(totalRecords)
	}

	// Memory delta
	allocDelta := memStatsAfter.Alloc - memStatsBefore.Alloc
	totalAllocDelta := memStatsAfter.TotalAlloc - memStatsBefore.TotalAlloc

	// Log comprehensive benchmark summary
	logger.Debugf("Thread[%s]: [PER_RECORD_WRITE_COMPLETE] "+
		"mode=PER_RECORD, total_records=%d, total_duration=%v, records_per_second=%.2f, "+
		"normalized_writes=%d, raw_writes=%d, "+
		"avg_write_time_per_record=%v, total_write_time_accum=%v, "+
		"new_partitions_created=%d, partition_processing_duration=%v, total_partitions=%d, "+
		"memory_alloc_delta_bytes=%d, memory_total_alloc_delta_bytes=%d",
		p.options.ThreadID,
		totalRecords, totalWriteDuration, recordsPerSecond,
		normalizedWriteCount, rawWriteCount,
		avgWriteTime, totalWriteDurationAccum,
		newPartitionsCreated, partitionCreationDuration, len(p.partitionedFiles),
		allocDelta, totalAllocDelta)

	return nil
}

// Check validates local paths and S3 credentials if applicable.
func (p *Parquet) Check(_ context.Context) error {
	uniqueSuffix := fmt.Sprintf("%d", time.Now().UnixNano())
	threadID := fmt.Sprintf("test_parquet_destination_%s", uniqueSuffix)

	p.options = &destination.Options{
		ThreadID: threadID,
	}

	// check for s3 writer configuration
	err := p.initS3Writer()
	if err != nil {
		return err
	}
	// test for s3 permissions
	if p.s3Client != nil {
		testKey := fmt.Sprintf("olake_writer_test/%s", utils.TimestampedFileName(".txt"))
		// Try to upload a small test file
		_, err = p.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(p.config.Bucket),
			Key:    aws.String(testKey),
			Body:   strings.NewReader("S3 write test"),
		})
		if err != nil {
			return fmt.Errorf("failed to write test file to S3: %s", err)
		}
		p.config.Path = os.TempDir()
		// trim '/' from prefix path
		p.config.Prefix = strings.Trim(p.config.Prefix, "/")
		logger.Infof("Thread[%s]: s3 writer configuration found", p.options.ThreadID)
	} else if p.config.Path != "" {
		logger.Infof("Thread[%s]: local writer configuration found, writing at location[%s]", p.options.ThreadID, p.config.Path)
	} else {
		return fmt.Errorf("invalid configuration found")
	}

	// Create the directory if it doesn't exist
	if err := os.MkdirAll(p.config.Path, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create path: %s", err)
	}

	// Test directory writability
	tempFile, err := os.CreateTemp(p.config.Path, "temporary-*.txt")
	if err != nil {
		return fmt.Errorf("directory is not writable: %s", err)
	}
	tempFile.Close()
	os.Remove(tempFile.Name())
	return nil
}

func (p *Parquet) closePqFiles() error {
	removeLocalFile := func(filePath, reason string) {
		err := os.Remove(filePath)
		if err != nil {
			logger.Warnf("Thread[%s]: Failed to delete file [%s], reason (%s): %s", p.options.ThreadID, filePath, reason, err)
			return
		}
		logger.Debugf("Thread[%s]: Deleted file [%s], reason (%s).", p.options.ThreadID, filePath, reason)
	}

	for basePath, parquetFile := range p.partitionedFiles {
		// construct full file path
		filePath := filepath.Join(p.config.Path, basePath, parquetFile.fileName)

		// Close writers
		var err error
		if p.stream.NormalizationEnabled() {
			err = parquetFile.writer.(*pqgo.GenericWriter[any]).Close()
		} else {
			err = parquetFile.writer.(*pqgo.GenericWriter[types.RawRecord]).Close()
		}
		if err != nil {
			return fmt.Errorf("failed to close writer: %s", err)
		}

		// Close file
		if err := parquetFile.file.Close(); err != nil {
			return fmt.Errorf("failed to close file: %s", err)
		}

		logger.Infof("Thread[%s]: Finished writing file [%s].", p.options.ThreadID, filePath)

		if p.s3Client != nil {
			// Open file for S3 upload
			file, err := os.Open(filePath)
			if err != nil {
				return fmt.Errorf("failed to open file: %s", err)
			}
			defer file.Close()

			// Construct S3 key path
			s3KeyPath := basePath
			if p.config.Prefix != "" {
				s3KeyPath = filepath.Join(p.config.Prefix, s3KeyPath)
			}
			s3KeyPath = filepath.Join(s3KeyPath, parquetFile.fileName)

			// Upload to S3 using multipart upload (automatically handles files > 5GB)
			_, err = p.s3Uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String(p.config.Bucket),
				Key:    aws.String(s3KeyPath),
				Body:   file,
			})
			if err != nil {
				return fmt.Errorf("failed to upload object to s3: %s", err)
			}

			// Remove local file after successful upload
			removeLocalFile(filePath, "uploaded to S3")
			logger.Infof("Thread[%s]: successfully uploaded file to S3: s3://%s/%s", p.options.ThreadID, p.config.Bucket, s3KeyPath)
		}
	}
	// make map empty
	p.partitionedFiles = make(map[string]*FileMetadata)
	return nil
}

func (p *Parquet) Close(_ context.Context) error {
	// --- LOG SUMMARY ---
	if p.totalRecordsSynced > 0 {
		logger.Infof("Thread[%s]: ========== BENCHMARK SUMMARY ==========", p.options.ThreadID)
		logger.Infof("Thread[%s]: Total Records Synced: %d", p.options.ThreadID, p.totalRecordsSynced)

		if p.totalBatchDuration > 0 {
			batchAvgRPS := float64(p.totalRecordsSynced) / p.totalBatchDuration.Seconds()
			logger.Infof("Thread[%s]: MODE: BATCH", p.options.ThreadID)
			logger.Infof("Thread[%s]: total_duration=%v, avg_rps=%.2f, total_memory_alloc_bytes=%d",
				p.options.ThreadID, p.totalBatchDuration, batchAvgRPS, p.totalBatchMemDelta)
		} else if p.totalPerRecordDuration > 0 {
			perRecordAvgRPS := float64(p.totalRecordsSynced) / p.totalPerRecordDuration.Seconds()
			logger.Infof("Thread[%s]: MODE: PER_RECORD", p.options.ThreadID)
			logger.Infof("Thread[%s]: total_duration=%v, avg_rps=%.2f, total_memory_alloc_bytes=%d",
				p.options.ThreadID, p.totalPerRecordDuration, perRecordAvgRPS, p.totalPerRecordMemDelta)
		}
		logger.Infof("Thread[%s]: ======================================", p.options.ThreadID)
	}

	return p.closePqFiles()
}

// validate schema change & evolution and removes null records
func (p *Parquet) FlattenAndCleanData(ctx context.Context, records []types.RawRecord) (bool, []types.RawRecord, any, error) {
	if !p.stream.NormalizationEnabled() {
		return false, records, nil, nil
	}

	if len(records) == 0 {
		return false, records, p.schema, nil
	}

	diffFound := atomic.Bool{} // to process records concurrently and detect schema difference

	err := utils.Concurrent(ctx, records, runtime.GOMAXPROCS(0)*16, func(_ context.Context, record types.RawRecord, idx int) error {
		// Add common fields
		records[idx].Data[constants.OlakeID] = record.OlakeID
		records[idx].Data[constants.OlakeTimestamp] = time.Now().UTC()
		records[idx].Data[constants.OpType] = record.OperationType
		if record.CdcTimestamp != nil {
			records[idx].Data[constants.CdcTimestamp] = *record.CdcTimestamp
		}

		flattenedRecord, err := typeutils.NewFlattener().Flatten(record.Data)
		if err != nil {
			return fmt.Errorf("failed to flatten record at index %d, pq writer: %s", idx, err)
		}

		// Store flattened result back to the record
		records[idx].Data = flattenedRecord

		if !diffFound.Load() {
			for columnName, columnValue := range flattenedRecord {
				detectedType := typeutils.TypeFromValue(columnValue)
				if _, columnExist := p.schema[columnName]; !columnExist {
					diffFound.Store(true)
					break
				}

				persistedTypes := p.schema[columnName].Types()
				if _, exist := utils.ArrayContains(persistedTypes, func(elem types.DataType) bool {
					return elem == detectedType
				}); !exist {
					diffFound.Store(true)
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		return false, nil, nil, fmt.Errorf("failed to process records: %s", err)
	}

	schemaChange := false // note: diff schema already detected so we can avoid this in future

	if diffFound.Load() {
		for _, record := range records {
			// Process the changes and upgrade new schema
			change, typeChange, _ := p.schema.Process(record.Data)
			schemaChange = change || typeChange || schemaChange
		}
	}

	return schemaChange, records, p.schema, utils.Concurrent(ctx, records, runtime.GOMAXPROCS(0)*16, func(_ context.Context, record types.RawRecord, _ int) error {
		return typeutils.ReformatRecord(p.schema, record.Data)
	})
}

// EvolveSchema updates the schema based on changes. Need to pass olakeTimestamp to get the correct partition path based on record ingestion time.
func (p *Parquet) EvolveSchema(_ context.Context, _, _ any) (any, error) {
	if !p.stream.NormalizationEnabled() {
		return false, nil
	}

	logger.Infof("Thread[%s]: schema evolution detected", p.options.ThreadID)

	// TODO: can we implement something https://github.com/parquet-go/parquet-go?tab=readme-ov-file#evolving-parquet-schemas-parquetconvert
	// close prev files as change detected (new files will be created with new schema)
	return p.schema.Clone(), p.closePqFiles()
}

// Type returns the type of the writer.
func (p *Parquet) Type() string {
	return string(types.Parquet)
}

func (p *Parquet) getPartitionedFilePath(values map[string]any, olakeTimestamp time.Time) string {
	pattern := p.stream.Self().StreamMetadata.PartitionRegex
	if pattern == "" {
		return p.basePath
	}
	// path pattern example /{col_name, 'fallback', granularity}/random_string/{col_name, fallback, granularity}
	patternRegex := regexp.MustCompile(constants.PartitionRegexParquet)

	// Replace placeholders
	result := patternRegex.ReplaceAllStringFunc(pattern, func(match string) string {
		trimmed := strings.Trim(match, "{}")
		regexVarBlock := strings.Split(trimmed, ",")

		if len(regexVarBlock) < 3 {
			return ""
		}

		colName := strings.TrimSpace(strings.Trim(regexVarBlock[0], `'`))
		defaultValue := strings.TrimSpace(strings.Trim(regexVarBlock[1], `'`))
		granularity := strings.TrimSpace(strings.Trim(regexVarBlock[2], `'`))

		if defaultValue == "" {
			defaultValue = fmt.Sprintf("default_%s", colName)
		}

		granularityFunction := func(value any) string {
			if granularity != "" {
				timestampInterface, err := typeutils.ReformatValue(types.Timestamp, value)
				if err == nil {
					timestamp, converted := timestampInterface.(time.Time)
					if converted {
						switch granularity {
						case "HH":
							value = fmt.Sprintf("%02d", timestamp.UTC().Hour())
						case "DD":
							value = fmt.Sprintf("%02d", timestamp.UTC().Day())
						case "WW":
							_, week := timestamp.UTC().ISOWeek()
							value = fmt.Sprintf("%02d", week)
						case "MM":
							value = fmt.Sprintf("%02d", int(timestamp.UTC().Month()))
						case "YYYY":
							value = timestamp.UTC().Year()
						}
					}
				} else {
					logger.Debugf("Thread[%s]: failed to convert value to timestamp: %s", p.options.ThreadID, err)
				}
			}
			return fmt.Sprintf("%v", value)
		}
		if colName == "now()" {
			return granularityFunction(olakeTimestamp)
		}
		value, exists := values[colName]
		if exists && value != nil {
			return granularityFunction(value)
		}
		return defaultValue
	})

	if result == "" {
		// use default for invalid partitions
		return p.basePath
	}
	return filepath.Join(p.basePath, strings.TrimSuffix(result, "/"))
}

func (p *Parquet) DropStreams(ctx context.Context, selectedStreams []types.StreamInterface) error {
	// check for s3 writer configuration
	err := p.initS3Writer()
	if err != nil {
		return err
	}

	if len(selectedStreams) == 0 {
		logger.Infof("no streams selected for clearing, skipping clear operation")
		return nil
	}

	paths := make([]string, 0, len(selectedStreams))
	for _, stream := range selectedStreams {
		paths = append(paths, stream.GetDestinationDatabase(nil)+"."+stream.GetDestinationTable())
	}

	if p.s3Client == nil {
		if err := p.clearLocalFiles(paths); err != nil {
			return fmt.Errorf("failed to clear local files: %s", err)
		}
	} else {
		if err := p.clearS3Files(ctx, paths); err != nil {
			return fmt.Errorf("failed to clear S3 files: %s", err)
		}
	}
	return nil
}

func (p *Parquet) clearLocalFiles(paths []string) error {
	for _, streamID := range paths {
		parts := strings.SplitN(streamID, ".", 2)
		if len(parts) != 2 {
			logger.Warnf("invalid stream ID format: %s, skipping", streamID)
			continue
		}
		namespace, tableName := parts[0], parts[1]
		streamPath := filepath.Join(p.config.Path, namespace, tableName)

		logger.Infof("clearing local path: %s", streamPath)

		if _, err := os.Stat(streamPath); os.IsNotExist(err) {
			logger.Debugf("local path does not exist, skipping: %s", streamPath)
			continue
		}

		if err := os.RemoveAll(streamPath); err != nil {
			return fmt.Errorf("failed to remove local path %s: %s", streamPath, err)
		}
	}

	return nil
}

func (p *Parquet) clearS3Files(ctx context.Context, paths []string) error {
	deleteS3PrefixStandard := func(filtPath string) error {
		iter := s3manager.NewDeleteListIterator(p.s3Client, &s3.ListObjectsInput{
			Bucket: aws.String(p.config.Bucket),
			Prefix: aws.String(filtPath),
		})

		if err := s3manager.NewBatchDeleteWithClient(p.s3Client).Delete(ctx, iter); err != nil {
			return fmt.Errorf("batch delete failed for filtPath %s: %s", filtPath, err)
		}
		return nil
	}

	for _, streamID := range paths {
		parts := strings.SplitN(streamID, ".", 2)
		if len(parts) != 2 {
			logger.Warnf("invalid stream ID format: %s, skipping", streamID)
			continue
		}
		prefix, namespace, tableName := strings.TrimLeft(p.config.Prefix, "/"), parts[0], parts[1]
		s3TablePath := filepath.Join(prefix, namespace, tableName, "/")

		logger.Debugf("clearing S3 prefix: s3://%s/%s", p.config.Bucket, s3TablePath)
		if err := deleteS3PrefixStandard(s3TablePath); err != nil {
			return fmt.Errorf("failed to clear S3 prefix %s: %s", s3TablePath, err)
		}

		logger.Debugf("successfully cleared S3 prefix: s3://%s/%s", p.config.Bucket, s3TablePath)
	}
	return nil
}

func init() {
	destination.RegisteredWriters[types.Parquet] = func() destination.Writer {
		return new(Parquet)
	}
}
