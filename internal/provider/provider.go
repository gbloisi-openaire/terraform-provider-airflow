package provider

import (
	"context"
	"crypto/tls"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/gbloisi-openaire/airflow-client-go/airflow"
	auth "github.com/gbloisi-openaire/airflow-client-go/auth"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/logging"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"

	"golang.org/x/oauth2"
)

type ProviderConfig struct {
	ApiClient   *airflow.APIClient
	AuthContext context.Context
}

func AirflowProvider() *schema.Provider {
	provider := &schema.Provider{
		Schema: map[string]*schema.Schema{
			"base_endpoint": {
				Type:         schema.TypeString,
				Required:     true,
				DefaultFunc:  schema.EnvDefaultFunc("AIRFLOW_BASE_ENDPOINT", nil),
				ValidateFunc: validation.IsURLWithHTTPorHTTPS,
			},
			"oauth2_token": {
				Type:          schema.TypeString,
				Optional:      true,
				Sensitive:     true,
				Description:   "The oauth to use for API authentication",
				DefaultFunc:   schema.EnvDefaultFunc("AIRFLOW_OAUTH2_TOKEN", nil),
				ConflictsWith: []string{"username", "password"},
			},
			"username": {
				Type:          schema.TypeString,
				DefaultFunc:   schema.EnvDefaultFunc("AIRFLOW_API_USERNAME", nil),
				Optional:      true,
				Description:   "The username to use for API basic authentication",
				RequiredWith:  []string{"password"},
				ConflictsWith: []string{"oauth2_token"},
			},
			"password": {
				Type:          schema.TypeString,
				DefaultFunc:   schema.EnvDefaultFunc("AIRFLOW_API_PASSWORD", nil),
				Optional:      true,
				Sensitive:     true,
				Description:   "The password to use for API basic authentication",
				RequiredWith:  []string{"username"},
				ConflictsWith: []string{"oauth2_token"},
			},
			"disable_ssl_verification": {
				Type:        schema.TypeBool,
				Optional:    true,
				Description: "Disable SSL verification",
				Default:     false,
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"airflow_connection": resourceConnection(),
			"airflow_dag":        resourceDag(),
			"airflow_dag_run":    resourceDagRun(),
			"airflow_variable":   resourceVariable(),
			"airflow_pool":       resourcePool(),
		},
		// ConfigureContextFunc: providerConfigure,
	}

	provider.ConfigureContextFunc = func(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
		return providerConfigure(ctx, d)
	}

	return provider
}

func providerConfigure(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	var transport http.RoundTripper

	if disableSSL := d.Get("disable_ssl_verification").(bool); disableSSL {
		transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	} else {
		transport = logging.NewLoggingHTTPTransport(http.DefaultTransport)
	}

	client := &http.Client{
		Transport: transport,
	}

	ctx = context.Background()
	endpoint := d.Get("base_endpoint").(string)
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, diag.Errorf("invalid base_endpoint: %s", err)
	}

	//path := strings.TrimSuffix(u.Path, "/")

	clientConf := &airflow.Configuration{
		Scheme:     u.Scheme,
		Host:       u.Host,
		Debug:      true,
		HTTPClient: client,
		Servers: airflow.ServerConfigurations{
			{
				URL:         strings.TrimSuffix(endpoint, "/"),
				Description: "Apache Airflow Stable API.",
			},
		},
	}

	if v, ok := d.GetOk("oauth2_token"); ok {
		ctx = context.WithValue(ctx, airflow.ContextOAuth2, oauth2.StaticTokenSource(&oauth2.Token{
			AccessToken: v.(string),
		}))
	}

	if username, ok := d.GetOk("username"); ok {
		var password interface{}
		if password, ok = d.GetOk("password"); !ok {
			return nil, diag.Errorf("found username for basic auth, but password not specified")
		}
		log.Printf("[DEBUG] Using API Basic Auth")

		loginBody := *auth.NewLoginBody(username.(string), password.(string))

		configuration := &auth.Configuration{
			Scheme:     u.Scheme,
			Host:       u.Host,
			Debug:      true,
			HTTPClient: client,
			Servers: auth.ServerConfigurations{
				{
					URL:         strings.TrimSuffix(endpoint, "/"),
					Description: "Apache Airflow Stable API.",
				},
			},
		}

		apiClient := auth.NewAPIClient(configuration)
		resp, _, err := apiClient.SimpleAuthManagerLoginAPI.CreateToken(ctx).LoginBody(loginBody).Execute()
		if err != nil {
			return nil, diag.Errorf("Error when calling `SimpleAuthManagerLoginAPI.CreateToken``: %v %v\n", err, endpoint)
		}

		tokenSource := oauth2.StaticTokenSource(&oauth2.Token{
			AccessToken: resp.AccessToken,
		})

		ctx = context.WithValue(ctx, airflow.ContextOAuth2, tokenSource)
	}

	prov := ProviderConfig{
		ApiClient:   airflow.NewAPIClient(clientConf),
		AuthContext: ctx,
	}

	return prov, diag.Diagnostics{}
}
