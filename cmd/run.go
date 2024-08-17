package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"cloud.google.com/go/storage"
	"github.com/alexhokl/helper/googleapi"
	"github.com/alexhokl/helper/iohelper"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/api/option"
)

type runBackupOptions struct {
	dryRun bool
}

var runBackupOpts runBackupOptions

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "start backup process",
	RunE:  runBackup,
}

func init() {
	rootCmd.AddCommand(runCmd)

	flags := runCmd.Flags()
	flags.BoolVar(&runBackupOpts.dryRun, "dry-run", false, "Dry run")
}

func runBackup(_ *cobra.Command, _ []string) error {
	ctx := context.Background()
	clientOptions := []option.ClientOption{}
	pathToApplicationDefaultCredentials := viper.GetString("path_to_application_default_credentials")
	if pathToApplicationDefaultCredentials != "" {
		clientOptions = append(clientOptions, option.WithCredentialsFile(pathToApplicationDefaultCredentials))
	}
	client, err := storage.NewClient(ctx, clientOptions...)
	if err != nil {
		return fmt.Errorf("unable to create storage client: %w", err)
	}
	bucketName := viper.GetString("bucket")
	machineAlias := viper.GetString("machine_alias")

	canAccessBucket, err := googleapi.IsBucketAccessible(ctx, client, bucketName)
	if err != nil {
		return fmt.Errorf("unable to determine if bucket [gs://%s] is accessible: %w", bucketName, err)
	}
	if !canAccessBucket {
		return fmt.Errorf("bucket [gs://%s] is not accessible; check if bucket exist or account has access to the bucket", bucketName)
	}

	paths := viper.GetStringSlice("paths")
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("unable to retrieve home directory: %w", err)
	}
	localFilePaths, err := ExtractFilePathsFromList(homeDir, paths)
	if err != nil {
		return fmt.Errorf("unable to extract local file paths: %w", err)
	}
	checksums, err := GetChecksums(localFilePaths)
	if err != nil {
		return fmt.Errorf("unable to generate checksums of local files: %w", err)
	}
	localPathsRequiredBackup, ignoredPaths, err := GetPathsRequiredBackup(ctx, client, bucketName, machineAlias, homeDir, checksums)
	if err != nil {
		return fmt.Errorf("unable to determine paths required backup: %w", err)
	}
	if len(ignoredPaths) > 0 && runBackupOpts.dryRun {
		for _, p := range ignoredPaths {
			fmt.Printf("Ignored path [%s]\n", p)
		}
	}
	if len(localPathsRequiredBackup) > 0 {
		for _, localPath := range localPathsRequiredBackup {
			relativePath, err := filepath.Rel(homeDir, localPath)
			if err != nil {
				return fmt.Errorf("unable to determine relative path of local file [%s]: %w", localPath, err)
			}
			if !runBackupOpts.dryRun {
				if err := CopyFileToBucket(ctx, client, bucketName, machineAlias, relativePath, localPath); err != nil {
					return fmt.Errorf("unable to upload file [%s] to bucket [gs://%s/%s/%s]: %w", localPath, bucketName, machineAlias, relativePath, err)
				}
			}
			if runBackupOpts.dryRun {
				fmt.Printf("File [%s] would be copied to bucket [gs://%s/%s/%s]\n", localPath, bucketName, machineAlias, relativePath)
			} else {
				fmt.Printf("Copied file [%s] to bucket [gs://%s/%s/%s]\n", localPath, bucketName, machineAlias, relativePath)
			}
		}
	}

	return nil
}

func CopyFileToBucket(ctx context.Context, storageClient *storage.Client, bucketName string, machineAlias string, relativePath string, localPath string) error {
	localFileBytes, err := iohelper.ReadBytesFromFile(localPath)
	if err != nil {
		return fmt.Errorf("unable to read local file [%s]: %w", localPath, err)
	}

	objectPath := path.Join(machineAlias, "/", relativePath)
	writer := storageClient.Bucket(bucketName).Object(objectPath).NewWriter(ctx)
	if _, err := writer.Write(localFileBytes); err != nil {
		return fmt.Errorf("unable to write to object [%s]: %w", objectPath, err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("unable to complete writing object [%s]: %w", objectPath, err)
	}
	return nil
}

func GetPathsRequiredBackup(ctx context.Context, storageClient *storage.Client, bucketName string, machineAlias string, parentPath string, checksums map[string]uint32) ([]string, []string, error) {
	requiredPaths := make([]string, 0)
	ignoredPaths := make([]string, 0)
	for localPath, localChecksum := range checksums {
		relativePath, err := filepath.Rel(parentPath, localPath)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to determine relative path of local file [%s]: %w", localPath, err)
		}
		objectPath := path.Join(machineAlias, "/", relativePath)
		required, err := IsBackupRequired(ctx, storageClient, bucketName, objectPath, localChecksum)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to determine if backup is required for file [%s]: %w", localPath, err)
		}
		if required {
			requiredPaths = append(requiredPaths, localPath)
		} else {
			ignoredPaths = append(ignoredPaths, localPath)
		}
	}
	return requiredPaths, ignoredPaths, nil
}

func IsBackupRequired(ctx context.Context, storageClient *storage.Client, bucketName string, objectPath string, localChecksum uint32) (bool, error) {
	exist, err := googleapi.IsBucketObjectExist(ctx, storageClient, bucketName, objectPath)
	if err != nil {
		return false, fmt.Errorf("unable to determine if object [%s] exists in bucket [gs://%s]: %w", objectPath, bucketName, err)
	}
	if !exist {
		return true, nil
	}
	remoteChecksum, err := googleapi.GetBucketObjectChecksum(ctx, storageClient, bucketName, objectPath)
	if err != nil {
		return false, fmt.Errorf("unable to retrieve checksum of object [%s] in bucket [gs://%s]: %w", objectPath, bucketName, err)
	}
	return localChecksum != remoteChecksum, nil
}

func GetChecksums(localPaths []string) (map[string]uint32, error) {
	checksums := make(map[string]uint32, len(localPaths))
	var errs []error
	for _, localPath := range localPaths {
		checksum, err := iohelper.GenerateCRC32Checksum(localPath)
		if err != nil {
			errs = append(errs, fmt.Errorf("unable to generate checksum of file [%s]: %w", localPath, err))
		} else {
			checksums[localPath] = checksum
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return checksums, nil
}

func GetRelativePaths(parentPath string, localPaths []string) ([]string, error) {
	relativePaths := make([]string, len(localPaths))
	for i, localPath := range localPaths {
		p, err := filepath.Rel(parentPath, localPath)
		if err != nil {
			return nil, fmt.Errorf("unable to determine relative path of local file [%s]: %w", localPath, err)
		}
		relativePaths[i] = p
	}
	return relativePaths, nil
}

func ExtractFilePathsFromList(parentPath string, relativePaths []string) ([]string, error) {
	var filePaths []string
	var errs []error
	for _, p := range relativePaths {
		paths, err := ExtractFilePaths(path.Join(parentPath, p))
		if err != nil {
			errs = append(errs, fmt.Errorf("unable to extract file paths from [%s]: %w", p, err))
		} else {
			filePaths = append(filePaths, paths...)
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return filePaths, nil
}

func ExtractFilePaths(localPath string) ([]string, error) {
	if iohelper.IsDirectoryExist(localPath) {
		files, err := os.ReadDir(localPath)
		if err != nil {
			return nil, fmt.Errorf("unable to read directory [%s]: %w", localPath, err)
		}
		filePaths := make([]string, 0)
		for _, file := range files {
			switch file.Type() {
			case os.ModeSymlink:
				continue
			case os.ModeSocket:
				continue
			case os.ModeDir:
				filesInDir, err := ExtractFilePaths(path.Join(localPath, file.Name()))
				if err != nil {
					return nil, err
				}
				filePaths = append(filePaths, filesInDir...)
				continue
			}
			filePaths = append(filePaths, path.Join(localPath, file.Name()))
		}
		return filePaths, nil
	} else if iohelper.IsFileExist(localPath) {
		return []string{localPath}, nil
	} else {
		return nil, fmt.Errorf("path [%s] does not exist", localPath)
	}
}
