package provider

import (
	"context"

	"github.com/gbloisi-openaire/airflow-client-go/airflow"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func resourcePool() *schema.Resource {
	return &schema.Resource{
		CreateWithoutTimeout: resourcePoolCreate,
		ReadWithoutTimeout:   resourcePoolRead,
		UpdateWithoutTimeout: resourcePoolUpdate,
		DeleteWithoutTimeout: resourcePoolDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"slots": {
				Type:     schema.TypeInt,
				Required: true,
			},
			"occupied_slots": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"used_slots": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"queued_slots": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"open_slots": {
				Type:     schema.TypeInt,
				Computed: true,
			},
		},
	}
}

func resourcePoolCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	name := d.Get("name").(string)
	slots := int32(d.Get("slots").(int))
	varApi := client.PoolAPI

	pool := airflow.PoolBody{
		Name:  name,
		Slots: slots,
	}

	_, resp, err := varApi.PostPool(pcfg.AuthContext).PoolBody(pool).Execute()
	if err != nil {
		if resp != nil && resp.StatusCode == 409 {
			// Try to fetch the existing pool to adopt it
			existingPool, _, getErr := varApi.GetPool(pcfg.AuthContext, name).Execute()
			if getErr != nil {
				return diag.Errorf("pool `%s` already exists, but failed to fetch it: %s", name, getErr)
			}

			// Adopt the existing pool
			d.SetId(existingPool.Name)

			// Only update if slots differ
			if existingPool.Slots != slots {
				return resourcePoolUpdate(ctx, d, m)
			}

			return resourcePoolRead(ctx, d, m)
		}

		return diag.Errorf("failed to create pool `%s` from Airflow: %s", name, err)
	}

	d.SetId(name)

	return resourcePoolRead(ctx, d, m)
}

func resourcePoolRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	pool, resp, err := client.PoolAPI.GetPool(pcfg.AuthContext, d.Id()).Execute()
	if resp != nil && resp.StatusCode == 404 {
		d.SetId("")
		return nil
	}
	if err != nil {
		return diag.Errorf("failed to get pool `%s` from Airflow: %s", d.Id(), err)
	}

	d.Set("name", pool.Name)
	d.Set("slots", pool.Slots)
	d.Set("occupied_slots", pool.OccupiedSlots)
	d.Set("queued_slots", pool.QueuedSlots)
	d.Set("open_slots", pool.OpenSlots)
	d.Set("used_slots", pool.RunningSlots)

	return nil
}

func resourcePoolUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	slots := int32(d.Get("slots").(int))
	name := d.Id()

	pool := airflow.PoolPatchBody{
		Slots: *airflow.NewNullableInt32(&slots),
	}

	_, _, err := client.PoolAPI.PatchPool(pcfg.AuthContext, name).PoolPatchBody(pool).UpdateMask([]string{"slots"}).Execute()
	if err != nil {
		return diag.Errorf("failed to update pool `%s` from Airflow: %s", name, err)
	}

	return resourcePoolRead(ctx, d, m)
}

func resourcePoolDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	if d.Id() == "default_pool" {
		// default pool cannot be deleted
		return nil
	}

	resp, err := client.PoolAPI.DeletePool(pcfg.AuthContext, d.Id()).Execute()
	if err != nil {
		return diag.Errorf("failed to delete pool `%s` from Airflow: %s", d.Id(), err)
	}

	if resp != nil && resp.StatusCode == 404 {
		return nil
	}

	return nil
}
