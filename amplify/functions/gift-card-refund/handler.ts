import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, ScanCommand, PutCommand, UpdateCommand, GetCommand } from '@aws-sdk/lib-dynamodb';
import { KMSClient, DecryptCommand } from '@aws-sdk/client-kms';
import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getMessaging } from 'firebase-admin/messaging';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const kms = new KMSClient({});

const ADMIN_TABLE = process.env.ADMIN_TABLE!;
const USER_TABLE = process.env.USER_TABLE!;

export const handler = async () => {
    console.log('[gift-card-refund] Checking for expired unclaimed gifts...');
    const now = new Date().toISOString();

    // Scan for pending gifts past their expiration
    // Note: For a production app at scale, a GSI on status/expiresAt would be better,
    // but a table scan is acceptable for the MVP phase where the table is small
    // and we only look for a specific subset periodically.
    let exclusiveStartKey: Record<string, any> | undefined;
    let expiredCount = 0;

    do {
        const res = await dynamo.send(new ScanCommand({
            TableName: ADMIN_TABLE,
            FilterExpression: '#sK = :sk AND begins_with(#pK, :giftPrefix) AND #status = :pending AND #expiresAt < :now',
            ExpressionAttributeNames: { '#sK': 'sK', '#pK': 'pK', '#status': 'status', '#expiresAt': 'expiresAt' },
            ExpressionAttributeValues: { ':sk': 'metadata', ':giftPrefix': 'GIFT#', ':pending': 'pending', ':now': now },
            ExclusiveStartKey: exclusiveStartKey,
        }));

        if (res.Items && res.Items.length > 0) {
            for (const gift of res.Items) {
                try {
                    // 1. Decrypt card details
                    const decryptRes = await kms.send(new DecryptCommand({
                        CiphertextBlob: Buffer.from(gift.encryptedCard as string, 'base64'),
                    }));
                    const { cardNumber, pin } = JSON.parse(Buffer.from(decryptRes.Plaintext!).toString('utf-8')) as { cardNumber: string; pin: string };

                    const cardSK = `GIFTCARD#refund#${gift.pK.split('#')[1]}`;

                    // 2. Write to sender's wallet
                    await dynamo.send(new PutCommand({
                        TableName: USER_TABLE,
                        Item: {
                            pK: `USER#${gift.senderPermULID}`,
                            sK: cardSK,
                            eventType: 'GIFTCARD',
                            status: 'ACTIVE',
                            primaryCat: 'gift_card',
                            brandId: gift.brandId,
                            desc: JSON.stringify({
                                brandName: gift.brandName,
                                brandId: gift.brandId,
                                cardNumber,
                                denomination: gift.denomination,
                                currency: gift.currency,
                                expiryDate: gift.expiresAt, // keep original expiry or whatever is appropriate
                                balance: gift.denomination,
                                source: 'refunded_gift',
                                isCustom: true, // We aren't doing live balance sync for refunded gifts as easily unless passing distributor info
                            }),
                            createdAt: now,
                            updatedAt: now,
                        },
                    }));

                    // 3. Delete or update the gift record
                    await dynamo.send(new UpdateCommand({
                        TableName: ADMIN_TABLE,
                        Key: { pK: gift.pK, sK: 'metadata' },
                        UpdateExpression: 'SET #status = :refunded REMOVE encryptedCard, ttl',
                        ExpressionAttributeNames: { '#status': 'status' },
                        ExpressionAttributeValues: { ':refunded': 'refunded' },
                    }));

                    // 4. Send Firebase push notification directly back to sender about the refund
                    await notifySender(gift.senderPermULID as string, gift.brandName as string, gift.denomination as number, cardSK, cardNumber, pin);

                    expiredCount++;
                } catch (err) {
                    console.error(`[gift-card-refund] Failed to process refund for ${gift.pK}`, err);
                }
            }
        }
        exclusiveStartKey = res.LastEvaluatedKey;
    } while (exclusiveStartKey);

    console.log(`[gift-card-refund] Processed ${expiredCount} expired gifts.`);
};

async function notifySender(permULID: string, brandName: string, denomination: number, cardSK: string, cardNumber: string, pin: string) {
    try {
        const tokenRes = await dynamo.send(new GetCommand({
            TableName: USER_TABLE,
            Key: { pK: `USER#${permULID}`, sK: 'DEVICE_TOKEN' },
        }));

        // We can extract token safely
        const descStr = tokenRes.Item?.desc as string | undefined;
        if (!descStr) return;

        let tokenStr: string;
        try {
            tokenStr = JSON.parse(descStr).token as string;
        } catch {
            return;
        }

        if (!tokenStr) return;

        if (getApps().length === 0) {
            const json = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
            if (!json) return;
            initializeApp({ credential: cert(JSON.parse(json)) });
        }

        await getMessaging().send({
            token: tokenStr,
            notification: {
                title: 'Unclaimed Gift Returned',
                body: `Your unclaimed ${brandName} gift card ($${denomination}) was returned to your wallet.`,
            },
            data: {
                type: 'GIFT_CARD_PIN', // Reuse same logic for delivering PIN
                cardSK,
                cardNumber,
                pin,
            },
        });
    } catch (err) {
        console.warn('[gift-card-refund] Failed to notify sender', err);
    }
}
