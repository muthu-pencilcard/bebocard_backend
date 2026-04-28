import { DynamoDBClient, ListTablesCommand } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import { createHash } from "crypto";

const client = new DynamoDBClient({ region: "us-east-1" });
const dynamo = DynamoDBDocumentClient.from(client);

function generateKeyData(keyId: string, secret: string) {
  const fullKey = `bebo_${keyId}.${secret}`;
  const keyHash = createHash("sha256").update(fullKey).digest("hex");
  return { fullKey, keyHash };
}

async function seed() {
  console.log("🌱 Seeding Phase 2 Tenant Registry...");
  
  const { TableNames } = await client.send(new ListTablesCommand({}));
  const refTable = TableNames?.find(t => t.includes("RefDataEvent"));

  if (!refTable) {
    console.error("❌ RefDataEvent table not found.");
    return;
  }

  // 1. Create the Woolworths Group Tenant
  await dynamo.send(new PutCommand({
    TableName: refTable,
    Item: {
      pK: "TENANT#woolworths_group",
      sK: "profile",
      status: "ACTIVE",
      brandIds: ["woolworths", "bigw", "bws"],
      allowedScopes: ["scan", "receipt", "segments"],
      desc: JSON.stringify({
        tenantName: "Woolworths Group",
        minCohortThreshold: 50,
        contactEmail: "bebo-onboarding@woolworths.com.au"
      }),
      createdAt: new Date().toISOString()
    }
  }));

  const wooliesKey = generateKeyData("SANDBOX_WOOLIES_KEY", "woolies_secret_123");
  await dynamo.send(new PutCommand({
    TableName: refTable,
    Item: {
      pK: "APIKEY#SANDBOX_WOOLIES_KEY",
      sK: "metadata",
      keyId: "SANDBOX_WOOLIES_KEY",
      tenantId: "woolworths_group",
      status: "ACTIVE",
      desc: JSON.stringify({
        keyHash: wooliesKey.keyHash,
        brandIds: ["woolworths", "bigw", "bws"], // Tenant-level scope
        allowedScopes: ["scan", "receipt", "segments"]
      }),
      createdAt: new Date().toISOString()
    }
  }));

  // 2. Nike Direct
  await dynamo.send(new PutCommand({
    TableName: refTable,
    Item: {
      pK: "TENANT#nike_direct",
      sK: "profile",
      status: "ACTIVE",
      brandIds: ["nike"],
      allowedScopes: ["scan", "receipt"],
      desc: JSON.stringify({ tenantName: "Nike Direct" }),
      createdAt: new Date().toISOString()
    }
  }));

  const nikeKey = generateKeyData("SANDBOX_NIKE_KEY", "nike_secret_123");
  await dynamo.send(new PutCommand({
    TableName: refTable,
    Item: {
      pK: "APIKEY#SANDBOX_NIKE_KEY",
      sK: "metadata",
      keyId: "SANDBOX_NIKE_KEY",
      tenantId: "nike_direct",
      status: "ACTIVE",
      desc: JSON.stringify({
        keyHash: nikeKey.keyHash,
        brandIds: ["nike"],
        allowedScopes: ["scan", "receipt"]
      }),
      createdAt: new Date().toISOString()
    }
  }));

  console.log("✅ Seed Data Ready.");
  console.log("🔑 Available Keys:");
  console.log(`📡 Woolworths: ${wooliesKey.fullKey}`);
  console.log(`📡 Nike:       ${nikeKey.fullKey}`);
}

seed().catch(console.error);
