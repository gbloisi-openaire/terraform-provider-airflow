package provider

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/gbloisi-openaire/airflow-client-go/airflow"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func resourceConnection() *schema.Resource {
	return &schema.Resource{
		CreateWithoutTimeout: resourceConnectionCreate,
		ReadWithoutTimeout:   resourceConnectionRead,
		UpdateWithoutTimeout: resourceConnectionUpdate,
		DeleteWithoutTimeout: resourceConnectionDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"connection_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"conn_type": {
				Type:     schema.TypeString,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"host": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"login": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"schema": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"port": {
				Type:         schema.TypeInt,
				Optional:     true,
				ValidateFunc: validation.IsPortNumberOrZero,
			},
			"password": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"extra": {
				Type:             schema.TypeString,
				DiffSuppressFunc: suppressSameJsonDiff,
				Optional:         true,
			},
		},
	}
}

func suppressSameJsonDiff(k, oldo, newo string, d *schema.ResourceData) bool {
	if strings.TrimSpace(oldo) == strings.TrimSpace(newo) {
		return true
	}

	var oldIface interface{}
	var newIface interface{}

	if err := json.Unmarshal([]byte(oldo), &oldIface); err != nil {
		return false
	}
	if err := json.Unmarshal([]byte(newo), &newIface); err != nil {
		return false
	}

	return reflect.DeepEqual(oldIface, newIface)
}

func resourceConnectionCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient
	connId := d.Get("connection_id").(string)
	connType := d.Get("conn_type").(string)

	conn := airflow.NewConnectionBody(
		connId,
		connType,
	)

	if v, ok := d.GetOk("host"); ok {
		conn.SetHost(v.(string))
	}

	if v, ok := d.GetOk("description"); ok {
		conn.SetDescription(v.(string))
	}

	if v, ok := d.GetOk("login"); ok {
		conn.SetLogin(v.(string))
	}

	if v, ok := d.GetOk("schema"); ok {
		conn.SetSchema(v.(string))
	}

	if v, ok := d.GetOk("port"); ok {
		conn.SetPort(int32(v.(int)))
	}

	conn.SetPassword(d.Get("password").(string))

	if v, ok := d.GetOk("extra"); ok {
		conn.SetExtra(v.(string))
	}

	connApi := client.ConnectionAPI

	_, res, err := connApi.PostConnection(pcfg.AuthContext).ConnectionBody(*conn).Execute()
	if err != nil {
		if res != nil && res.StatusCode == 409 {
			// Try to fetch the existing pool to adopt it
			existingConnection, _, getErr := connApi.GetConnection(pcfg.AuthContext, connId).Execute()
			if getErr != nil {
				return diag.Errorf("connection `%s` already exists, but failed to fetch it: %s", connId, getErr)
			}

			// Adopt the existing variable
			d.SetId(existingConnection.ConnectionId)

			// Always try to update to be indempotent
			return resourceConnectionUpdate(ctx, d, m)
		}

		return diag.Errorf("failed to create connection `%s` from Airflow: %s", connId, err)
	}
	d.SetId(connId)

	return resourceConnectionRead(ctx, d, m)
}

func resourceConnectionRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient
	connection, resp, err := client.ConnectionAPI.GetConnection(pcfg.AuthContext, d.Id()).Execute()
	if resp != nil && resp.StatusCode == 404 {
		d.SetId("")
		return nil
	}
	if err != nil {
		return diag.Errorf("failed to get connection `%s` from Airflow: %s", d.Id(), err)
	}

	d.Set("connection_id", connection.GetConnectionId())
	d.Set("conn_type", connection.GetConnType())
	d.Set("host", connection.GetHost())
	d.Set("login", connection.GetLogin())
	d.Set("schema", connection.GetSchema())
	d.Set("port", connection.GetPort())
	d.Set("extra", connection.GetExtra())
	d.Set("description", connection.GetDescription())

	if v, ok := connection.GetPasswordOk(); ok {
		d.Set("password", v)
	} else if v, ok := d.GetOk("password"); ok {
		d.Set("password", v)
	}

	return nil
}

func resourceConnectionUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient
	connId := d.Id()
	connType := d.Get("conn_type").(string)

	conn := airflow.NewConnectionBody(
		connId,
		connType,
	)

	if v, ok := d.GetOk("host"); ok {
		conn.SetHost(v.(string))
	} else {
		conn.SetHostNil()
	}

	if v, ok := d.GetOk("description"); ok {
		conn.SetDescription(v.(string))
	} else {
		conn.SetDescriptionNil()
	}

	if v, ok := d.GetOk("login"); ok {
		conn.SetLogin(v.(string))
	} else {
		conn.SetLoginNil()
	}

	if v, ok := d.GetOk("schema"); ok {
		conn.SetSchema(v.(string))
	} else {
		conn.SetSchemaNil()
	}

	if v, ok := d.GetOk("port"); ok {
		conn.SetPort(int32(v.(int)))
	} else {
		conn.SetPortNil()
	}

	if v, ok := d.GetOk("password"); ok && v.(string) != "" {
		conn.SetPassword(v.(string))
	}

	if v, ok := d.GetOk("extra"); ok {
		conn.SetExtra(v.(string))
	} else {
		conn.SetExtraNil()
	}

	_, _, err := client.ConnectionAPI.PatchConnection(pcfg.AuthContext, connId).ConnectionBody(*conn).Execute()
	if err != nil {
		return diag.Errorf("failed to update connection `%s` from Airflow: %s", connId, err)
	}

	return resourceConnectionRead(ctx, d, m)
}

func resourceConnectionDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	resp, err := client.ConnectionAPI.DeleteConnection(pcfg.AuthContext, d.Id()).Execute()
	if err != nil {
		return diag.Errorf("failed to delete connection `%s` from Airflow: %s", d.Id(), err)
	}

	if resp != nil && resp.StatusCode == 404 {
		return nil
	}

	return nil
}
