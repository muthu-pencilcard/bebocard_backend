import { defineFunction } from '@aws-amplify/backend';

export const tenantProvisionerFn = defineFunction({
  resourceGroupName: "data",
  name: 'tenant-provisioner',
  entry: './handler.ts',
  timeoutSeconds: 300, // Provisioning Glue tables can take a few seconds
  memoryMB: 512,
});
