package provider

import (
	"context"

	"github.com/gbloisi-openaire/airflow-client-go/airflow"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func resourceVariable() *schema.Resource {
	return &schema.Resource{
		CreateWithoutTimeout: resourceVariableCreate,
		ReadWithoutTimeout:   resourceVariableRead,
		UpdateWithoutTimeout: resourceVariableUpdate,
		DeleteWithoutTimeout: resourceVariableDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"key": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"value": {
				Type:     schema.TypeString,
				Required: true,
			},
		},
	}
}

func resourceVariableCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	key := d.Get("key").(string)
	val := d.Get("value").(string)

	varApi := client.VariableAPI

	variableReq := airflow.VariableBody{
		Key:   key,
		Value: val,
	}

	if v, ok := d.GetOk("description"); ok {
		variableReq.SetDescription(v.(string))
	}

	_, res, err := varApi.PostVariable(pcfg.AuthContext).VariableBody(variableReq).Execute()
	if err != nil {
		if res.StatusCode == 409 || res.Status == "409 Conflict" {
			// Try to fetch the existing pool to adopt it
			existingVariable, _, getErr := varApi.GetVariable(pcfg.AuthContext, key).Execute()
			if getErr != nil {
				return diag.Errorf("variable `%s` already exists, but failed to fetch it: %s", key, getErr)
			}

			// Adopt the existing variable
			d.SetId(existingVariable.Key)

			// Only update if values differ
			if existingVariable.Value != variableReq.Key || existingVariable.Description != variableReq.Description {
				return resourceVariableUpdate(ctx, d, m)
			}

			return resourceVariableRead(ctx, d, m)
		}

		return diag.Errorf("failed to create variable `%s`, Status: `%s` from Airflow: %s", key, res.Status, err)
	}

	d.SetId(key)

	return resourceVariableRead(ctx, d, m)
}

func resourceVariableRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	variable, resp, err := client.VariableAPI.GetVariable(pcfg.AuthContext, d.Id()).Execute()
	if resp != nil && resp.StatusCode == 404 {
		d.SetId("")
		return nil
	}
	if err != nil {
		return diag.Errorf("failed to get variable `%s`, Status: `%s` from Airflow: %s", d.Id(), resp.Status, err)
	}

	d.Set("key", variable.Key)
	d.Set("value", variable.Value)
	d.Set("description", variable.GetDescription())

	return nil
}

func resourceVariableUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	val := d.Get("value").(string)
	key := d.Id()

	variableReq := airflow.VariableBody{
		Key:   key,
		Value: val,
	}

	if v, ok := d.GetOk("description"); ok {
		variableReq.SetDescription(v.(string))
	}

	_, resp, err := client.VariableAPI.PatchVariable(pcfg.AuthContext, key).VariableBody(variableReq).Execute()
	if err != nil {
		return diag.Errorf("failed to update variable `%s`, Status: `%s` from Airflow: %s", key, resp.Status, err)
	}

	return resourceVariableRead(ctx, d, m)
}

func resourceVariableDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	resp, err := client.VariableAPI.DeleteVariable(pcfg.AuthContext, d.Id()).Execute()
	if err != nil {
		return diag.Errorf("failed to delete variable `%s`, Status: `%s` from Airflow: %s", d.Id(), resp.Status, err)
	}

	if resp != nil && resp.StatusCode == 404 {
		return nil
	}

	return nil
}
