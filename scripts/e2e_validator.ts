import { DynamoDBClient, ListTablesCommand } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({ region: "us-east-1" });
const dynamo = DynamoDBDocumentClient.from(client);

async function getAllBeboTables() {
  const { TableNames } = await client.send(new ListTablesCommand({}));
  const userTables = TableNames?.filter(t => t.includes("UserDataEvent")) || [];
  const adminTables = TableNames?.filter(t => t.includes("AdminDataEvent")) || [];
  return { userTables, adminTables };
}

async function runTest() {
  console.log("\n🔍 BeboCard Multi-Table E2E Validator");
  console.log("==========================================");

  const { userTables, adminTables } = await getAllBeboTables();
  
  if (!userTables.length) {
    console.error("❌ No UserDataEvent tables found.");
    return;
  }

  console.log(`📡 Found ${userTables.length} potential user tables. Searching for data...`);

  let bestIdentity: any = null;
  let bestTable: string | null = null;

  for (const table of userTables) {
    const res = await dynamo.send(new ScanCommand({
      TableName: table,
      FilterExpression: "sK = :sk",
      ExpressionAttributeValues: { ":sk": "IDENTITY" },
    })).catch(() => ({ Items: [] }));

    if (res.Items?.length) {
      const latestInTable = res.Items.sort((a, b) => (b.updatedAt || "").localeCompare(a.updatedAt || ""))[0];
      if (!bestIdentity || (latestInTable.updatedAt || "").localeCompare(bestIdentity.updatedAt || "") > 0) {
        bestIdentity = latestInTable;
        bestTable = table;
      }
    }
  }

  if (!bestTable || !bestIdentity) {
    console.log("❌ No identity records found in ANY UserDataEvent tables.");
    return;
  }
  
  const activeUserTable = bestTable;
  const latestIdentity = bestIdentity;
  console.log(`✅ FOUND LATEST DATA in table: ${activeUserTable}`);

  // Find corresponding Admin table (Amplify tables usually share the same suffix)
  const suffix = activeUserTable.split("-")[1];
  const activeAdminTable = adminTables.find(t => t.includes(suffix)) || adminTables[0];

  const permULID = latestIdentity.pK.replace("USER#", "");
  const secondaryULID = latestIdentity.secondaryULID;

  console.log(`✅ User ID: ${permULID}`);
  console.log(`✅ Barcode: ${secondaryULID}`);

  // 2. Verify Scan Routing
  console.log("\nSTEP 2: Verifying Scan Routing...");
  const scanRecord = await dynamo.send(new GetCommand({
    TableName: activeAdminTable,
    Key: {
      pK: `SCAN#${secondaryULID}`,
      sK: permULID
    }
  }));

  if (scanRecord.Item) {
    console.log(`✅ SUCCESS: Barcode resolves correctly in ${activeAdminTable}`);
  } else {
    console.log(`❌ FAILURE: Barcode not found in ${activeAdminTable}`);
  }

  console.log("\n==========================================");
}

runTest().catch(console.error);
