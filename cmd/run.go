package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"cloud.google.com/go/storage"
	"github.com/alexhokl/helper/iohelper"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	bucketName := viper.GetString("bucket")
	machineAlias := viper.GetString("machine_alias")

	paths := viper.GetStringSlice("paths")
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	localFilePaths, err := ExtractFilePathsFromList(homeDir, paths)
	if err != nil {
		return err
	}
	checksums, err := GetChecksums(localFilePaths)
	if err != nil {
		return err
	}
	localPathsRequiredBackup, ignoredPaths, err := GetPathsRequiredBackup(ctx, client, bucketName, machineAlias, homeDir, checksums)
	if err != nil {
		return err
	}
	if len(ignoredPaths) > 0 {
		for _, p := range ignoredPaths {
			fmt.Printf("Ignored path [%s]\n", p)
		}
	}
	if len(localPathsRequiredBackup) > 0 {
		for _, localPath := range localPathsRequiredBackup {
			relativePath, err := filepath.Rel(homeDir, localPath)
			if err != nil {
				return err
			}
			if !runBackupOpts.dryRun {
				if err := CopyFileToBucket(ctx, client, bucketName, machineAlias, relativePath, localPath); err != nil {
					return err
				}
			}
			fmt.Printf("Copied file [%s] to bucket [gs://%s/%s/%s]\n", localPath, bucketName, machineAlias, relativePath)
		}
	}

	return nil
}

func CopyFileToBucket(ctx context.Context, storageClient *storage.Client, bucketName string, machineAlias string, relativePath string, localPath string) error {
	localFileBytes, err := iohelper.ReadBytesFromFile(localPath)
	if err != nil {
		return err
	}

	objectPath := path.Join(machineAlias, "/", relativePath)
	writer := storageClient.Bucket(bucketName).Object(objectPath).NewWriter(ctx)
	if _, err := writer.Write(localFileBytes); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	return nil
}

func GetPathsRequiredBackup(ctx context.Context, storageClient *storage.Client, bucketName string, machineAlias string, parentPath string, checksums map[string]uint32) ([]string, []string, error) {
	requiredPaths := make([]string, 0)
	ignoredPaths := make([]string, 0)
	for localPath, localChecksum := range checksums {
		relativePath, err := filepath.Rel(parentPath, localPath)
		if err != nil {
			return nil, nil, err
		}
		objectPath := path.Join(machineAlias, "/", relativePath)
		required, err := IsBackupRequired(ctx, storageClient, bucketName, objectPath, localChecksum)
		if err != nil {
			return nil, nil, err
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
	exist, err := IsObjectExist(ctx, storageClient, bucketName, objectPath)
	if err != nil {
		return false, err
	}
	if !exist {
		return true, nil
	}
	remoteChecksum, err := GetObjectChecksum(ctx, storageClient, bucketName, objectPath)
	if err != nil {
		return false, err
	}
	return localChecksum != remoteChecksum, nil
}

func GetChecksums(localPaths []string) (map[string]uint32, error) {
	checksums := make(map[string]uint32, len(localPaths))
	var errs []error
	for _, localPath := range localPaths {
		checksum, err := iohelper.GenerateCRC32Checksum(localPath)
		if err != nil {
			errs = append(errs, err)
		} else {
			checksums[localPath] = checksum
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return checksums, nil
}

func IsObjectExist(ctx context.Context, storageClient *storage.Client, bucketName string, objectPath string) (bool, error) {
	_, err := storageClient.Bucket(bucketName).Object(objectPath).Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func GetObjectChecksum(ctx context.Context, storageClient *storage.Client, bucketName string, objectPath string) (uint32, error) {
	attrs, err := storageClient.Bucket(bucketName).Object(objectPath).Attrs(ctx)
	if err != nil {
		return 0, err
	}
	return attrs.CRC32C, nil
}

func GetRelativePaths(parentPath string, localPaths []string) ([]string, error) {
	relativePaths := make([]string, len(localPaths))
	for i, localPath := range localPaths {
		p, err := filepath.Rel(parentPath, localPath)
		if err != nil {
			return nil, err
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
			errs = append(errs, err)
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
			return nil, err
		}
		filePaths := make([]string, len(files))
		for i, file := range files {
			filePaths[i] = path.Join(localPath, file.Name())
		}
		return filePaths, nil
	} else if iohelper.IsFileExist(localPath) {
		return []string{localPath}, nil
	} else {
		return nil, fmt.Errorf("path [%s] does not exist", localPath)
	}
}
