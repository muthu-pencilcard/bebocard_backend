import type { ScheduledHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
    DynamoDBDocumentClient,
    ScanCommand,
    GetCommand,
    PutCommand,
} from '@aws-sdk/lib-dynamodb';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const USER_TABLE = process.env.USER_TABLE!;
const REF_TABLE = process.env.REF_TABLE!;

export const handler: ScheduledHandler = async () => {
    try {
        const now = new Date().toISOString();

        // Scan for all active subscriptions
        const res = await dynamo.send(new ScanCommand({
            TableName: USER_TABLE,
            FilterExpression: '(#et = :sub OR #et = :rec) AND #s = :active',
            ExpressionAttributeNames: {
                '#et': 'eventType',
                '#s': 'status',
            },
            ExpressionAttributeValues: {
                ':sub': 'SUBSCRIPTION',
                ':rec': 'RECURRING',
                ':active': 'ACTIVE',
            },
        }));

        const items = res.Items ?? [];
        console.log(`[subscription-negotiator] Found ${items.length} active subscriptions`);

        for (const item of items) {
            const desc = JSON.parse(item.desc ?? '{}');
            const amount = Number(desc.amount) || 0.0;

            // We only care about amount > 50 AUD
            if (amount <= 50) continue;

            const brandId = desc.brandId as string | undefined;
            const brandName = desc.brandName as string ?? brandId ?? 'Brand';
            if (!brandId) continue;

            // Look up benchmark in REFDATA_TABLE
            const refRes = await dynamo.send(new GetCommand({
                TableName: REF_TABLE,
                Key: { pK: `BENCHMARK#${brandId}`, sK: 'BENCHMARK' },
            }));

            // If no benchmark, we can't negotiate
            if (!refRes.Item) continue;

            const benchmarkAmount = refRes.Item.benchmarkAmount as number;

            // If user amount > 15% higher than benchmark
            if (amount > benchmarkAmount * 1.15) {
                const potentialSaving = amount - benchmarkAmount;

                // Write event to USER_TABLE
                const permULID = (item.pK as string).replace('USER#', '');
                const savingSK = `SAVING_OPPORTUNITY#${brandId}`;

                await dynamo.send(new PutCommand({
                    TableName: USER_TABLE,
                    Item: {
                        pK: item.pK,
                        sK: savingSK,
                        eventType: 'SAVING_OPPORTUNITY',
                        status: 'ACTIVE',
                        primaryCat: 'saving_opportunity',
                        subCategory: brandId,
                        desc: JSON.stringify({
                            brandId,
                            brandName,
                            subId: desc.subId ?? 'manual',
                            userAmount: amount,
                            benchmarkAmount,
                            potentialSaving,
                            detectedAt: now,
                        }),
                        createdAt: now,
                        updatedAt: now,
                    },
                }));

                // Send FCM Push
                const tokenRes = await dynamo.send(new GetCommand({
                    TableName: USER_TABLE,
                    Key: { pK: item.pK, sK: 'DEVICE_TOKEN' },
                }));

                const token = tokenRes.Item ? (JSON.parse(tokenRes.Item.desc ?? '{}') as { token?: string }).token : null;
                if (token) {
                    try {
                        const { initializeApp, getApps, cert } = await import('firebase-admin/app');
                        const { getMessaging } = await import('firebase-admin/messaging');
                        if (getApps().length === 0) {
                            const sa = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
                            if (sa) initializeApp({ credential: cert(JSON.parse(sa)) });
                        }
                        await getMessaging().send({
                            token,
                            data: {
                                type: 'SAVING_OPPORTUNITY',
                                brandId,
                            },
                            notification: {
                                title: 'Saving Opportunity',
                                body: `You could save $${potentialSaving.toFixed(2)} on your ${brandName} subscription`,
                            },
                            android: { priority: 'normal' },
                            apns: { payload: { aps: { contentAvailable: true } } },
                        });
                    } catch (e) {
                        console.error('[subscription-negotiator] FCM push failed', e);
                    }
                }
            }
        }
    } catch (err) {
        console.error('[subscription-negotiator] failed', err);
    }
};
