package provider

import (
	"context"

	"github.com/gbloisi-openaire/airflow-client-go/airflow"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func resourceDag() *schema.Resource {
	return &schema.Resource{
		CreateWithoutTimeout: resourceDagUpdate,
		ReadWithoutTimeout:   resourceDagRead,
		UpdateWithoutTimeout: resourceDagUpdate,
		DeleteWithoutTimeout: resourceDagDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"dag_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"description": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"delete_dag": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"file_token": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"fileloc": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"is_active": {
				Type:     schema.TypeBool,
				Computed: true,
			},
			"is_paused": {
				Type:     schema.TypeBool,
				Required: true,
			},
			"is_subdag": {
				Type:     schema.TypeBool,
				Computed: true,
			},
			"root_dag_id": {
				Type:     schema.TypeString,
				Computed: true,
			},
		},
	}
}

func resourceDagUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	dagId := d.Get("dag_id").(string)
	dagApi := client.DAGAPI
	dag := airflow.DAGPatchBody{
		IsPaused: d.Get("is_paused").(bool),
	}
	dag.SetIsPaused(d.Get("is_paused").(bool))

	_, res, err := dagApi.PatchDag(pcfg.AuthContext, dagId).DAGPatchBody(dag).Execute()
	if res.StatusCode != 200 {
		return diag.Errorf("failed to update DAG `%s` from Airflow: %s", dagId, err)
	}
	d.SetId(dagId)

	return resourceDagRead(ctx, d, m)
}

func resourceDagRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	DAG, resp, err := client.DAGAPI.GetDag(pcfg.AuthContext, d.Id()).Execute()
	if resp != nil && resp.StatusCode == 404 {
		d.SetId("")
		return nil
	}
	if resp.StatusCode != 200 {
		return diag.Errorf("failed to get DAG `%s` from Airflow: %s", d.Id(), err)
	}

	d.Set("dag_id", DAG.DagId)
	d.Set("is_paused", DAG.IsPaused)
	d.Set("is_active", !DAG.IsPaused && !DAG.IsStale)
	//d.Set("is_subdag", DAG.IsSubdag)
	d.Set("description", DAG.Description.Get())
	d.Set("file_token", DAG.FileToken)
	d.Set("fileloc", DAG.Fileloc)
	//d.Set("root_dag_id", DAG.RootDagId.Get())

	return nil
}

func resourceDagDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient.DAGAPI

	if d.Get("delete_dag").(bool) {

		_, resp, err := client.DeleteDag(pcfg.AuthContext, d.Id()).Execute()
		if err != nil {
			return diag.Errorf("failed to delete DAG `%s` from Airflow: %s", d.Id(), err)
		}

		if resp != nil && resp.StatusCode == 404 {
			return nil
		}
	}

	return nil
}
