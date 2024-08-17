package cmd

import (
	"fmt"

	"github.com/alexhokl/helper/cli"
	"github.com/alexhokl/helper/iohelper"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:               "gcp-backup",
	Short:             "Backup files of this machine to Google Cloud Storage",
	SilenceUsage:      true,
	PersistentPreRunE: validateBucketConnection,
}

func Execute() {
	rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.gcp-backup.yml)")
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func initConfig() {
	cli.ConfigureViper(cfgFile, "gcp-backup", false, "")
}

func validateBucketConnection(cmd *cobra.Command, _ []string) error {
	bucketName := viper.GetString("bucket")
	if bucketName == "" {
		return fmt.Errorf("bucket name has not been configured")
	}
	machineAlias := viper.GetString("machine_alias")
	if machineAlias == "" {
		return fmt.Errorf("machine alias has not been configured")
	}
	pathToApplicationDefaultCredentials := viper.GetString("path_to_application_default_credentials")
	if pathToApplicationDefaultCredentials != "" {
		if !iohelper.IsFileExist(pathToApplicationDefaultCredentials) {
			return fmt.Errorf("path [%s] application default credentials does not exist", pathToApplicationDefaultCredentials)
		}
	}

	return nil
}
