import { SSMClient, PutParameterCommand } from "@aws-sdk/client-ssm";

async function setInRegion(region, profile) {
  process.env.AWS_PROFILE = profile;
  const client = new SSMClient({ region });
  const appId = "d3ezb0a2f7pbij";
  const branch = "main";
  const secretName = "GLOBAL_ANALYTICS_SALT";
  const secretValue = "1a2b3c4d5e6f7g8h9i0j1a2b3c4d5e6f";

  const name = `/amplify/${appId}/${branch}/${secretName}`;
  const command = new PutParameterCommand({
    Name: name,
    Value: secretValue,
    Type: "SecureString",
    Overwrite: true,
  });

  try {
    await client.send(command);
    console.log(`Success in region ${region} with profile ${profile}`);
  } catch (err) {
    console.error(`Error in region ${region}:`, err.message);
  }
}

async function main() {
  await setInRegion("ap-southeast-2", "muthuPencilCard");
  await setInRegion("us-east-1", "muthuPencilCard");
  await setInRegion("ap-southeast-2", "default");
  await setInRegion("us-east-1", "default");
}

main();
