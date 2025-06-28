package main

import (
	"flag"

	"github.com/gbloisi-openaire/terraform-provider-airflow/internal/provider"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"
)

func main() {
	var debug bool

	flag.BoolVar(&debug, "debug", false, "set to true to run the provider with support for debuggers like delve")
	flag.Parse()

	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: func() *schema.Provider {
			return provider.AirflowProvider()
		},
		ProviderAddr: "registry.terraform.io/drfaust92/airflow",
		Debug:        debug,
	})
}
