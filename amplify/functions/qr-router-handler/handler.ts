import type { APIGatewayProxyHandler, APIGatewayProxyResult } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand } from '@aws-sdk/lib-dynamodb';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const REFDATA_TABLE = process.env.REFDATA_TABLE!;

export const handler: APIGatewayProxyHandler = async (event) => {
  const { pathParameters, headers, path } = event;
  const brandId = pathParameters?.brandId;
  const storeId = pathParameters?.storeId; // If provided via /{brandId}/{storeId}
  const userAgent = headers['User-Agent'] || headers['user-agent'] || '';
  
  // 301 Redirect for base path
  if (!brandId) {
    return {
      statusCode: 302,
      headers: { Location: 'https://bebocard.com.au' },
      body: '',
    } as APIGatewayProxyResult;
  }

  // Lookup brand profile to get name and theme colors
  const brandRes = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: 'profile' },
  }));

  const brandProfile = JSON.parse(brandRes.Item?.desc ?? '{}');
  const brandName = brandProfile.brandName || brandProfile.name || brandId;
  const brandColor = brandProfile.brandColor || brandProfile.color || '#6366F1';
  const logoKey = brandProfile.logoKey;

  const isIos = /iPhone|iPad|iPod/i.test(userAgent);
  const isAndroid = /Android/i.test(userAgent);

  // Deep Link URI for existing users (App Link / Universal Link registration handles this usually)
  const deepLink = `bebocard://scan/brand/${brandId}${storeId ? `?storeId=${storeId}` : ''}`;
  
  // Store URLs
  const iosStore = 'https://apps.apple.com/au/app/bebocard/id123456789';
  const androidStore = 'https://play.google.com/store/apps/details?id=me.bebocard.app';

  // Lightweight Landing Page for acquisition / anonymous scan
  const html = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>${brandName} | BeboCard</title>
    <style>
        :root { --brand-color: ${brandColor}; --bg: #000; --text: #fff; --card-bg: #111; }
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background: var(--bg); color: var(--text); margin: 0; display: flex; flex-direction: column; align-items: center; justify-content: center; min-height: 100vh; text-align: center; padding: 20px; box-sizing: border-box; }
        .card { background: var(--card-bg); border: 1px solid #222; border-radius: 32px; padding: 40px 24px; text-align: center; max-width: 360px; width: 100%; box-shadow: 0 20px 50px rgba(0,0,0,0.5); }
        .logo { width: 80px; height: 80px; background: var(--brand-color); border-radius: 20px; margin: 0 auto 24px; display: flex; align-items: center; justify-content: center; font-size: 32px; font-weight: 900; overflow: hidden; }
        .logo img { width: 100%; height: 100%; object-fit: cover; }
        h1 { font-size: 24px; font-weight: 900; margin: 0 0 12px; letter-spacing: -0.01em; }
        p { color: #888; font-size: 15px; line-height: 1.5; margin: 0 0 32px; }
        .btn { display: block; width: 100%; background: #fff; color: #000; text-decoration: none; padding: 18px; border-radius: 18px; font-weight: 700; font-size: 16px; margin-bottom: 12px; transition: transform 0.1s; border: none; cursor: pointer; box-sizing: border-box; }
        .btn:active { transform: scale(0.97); }
        .btn-secondary { background: transparent; color: #888; border: 1px solid #333; }
        .input-group { margin-bottom: 24px; text-align: left; }
        .input-label { display: block; font-size: 11px; font-weight: 800; color: #555; text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 8px; margin-left: 4px; }
        input { width: 100%; background: #1a1a1a; border: 1px solid #333; border-radius: 12px; padding: 14px; color: white; font-size: 16px; box-sizing: border-box; outline: none; transition: border-color 0.2s; }
        input:focus { border-color: var(--brand-color); }
        .footer { margin-top: 40px; font-size: 11px; color: #444; font-weight: 600; text-transform: uppercase; letter-spacing: 0.1em; }
    </style>
</head>
<body>
    <div class="card" id="main-content">
        <div class="logo">
            ${logoKey ? `<img src="https://cdn.bebocard.com/${logoKey}" alt="${brandName}">` : brandName.charAt(0)}
        </div>
        <h1>Welcome to ${brandName}</h1>
        <p>Scan to earn points and get digital receipts instantly with BeboCard.</p>
        
        <div class="input-group">
            <span class="input-label">Digital Receipt Email</span>
            <input type="email" id="email" placeholder="you@example.com" inputmode="email">
        </div>

        <button class="btn" onclick="handleJoin()">Get Digital Receipt</button>
        <a href="${isAndroid ? androidStore : iosStore}" class="btn btn-secondary">Download BeboCard</a>
    </div>

    <div class="footer">
        Powered by BeboCard &bull; Privacy First
    </div>

    <script>
        // Automatic Deep Link Attempt
        setTimeout(() => {
            window.location = "${deepLink}";
        }, 300);

        async function handleJoin() {
            const email = document.getElementById('email').value;
            if (!email || !email.includes('@')) {
                alert('Please enter a valid email address.');
                return;
            }

            const btn = document.querySelector('.btn');
            btn.innerText = 'Processing...';
            btn.disabled = true;

            try {
                // In Phase 2, this will call P1-9 anonymous receipt endpoint
                console.log('Registering anonymous interest:', { brandId: '${brandId}', storeId: '${storeId}', email });
                
                setTimeout(() => {
                    document.getElementById('main-content').innerHTML = \`
                        <div class="logo" style="background: #10B981">✅</div>
                        <h1>Done!</h1>
                        <p>Your digital receipt will be sent to <b>\${email}</b>.</p>
                        <a href="${isAndroid ? androidStore : iosStore}" class="btn">Complete Member Profile</a>
                        <p style="font-size: 12px; color: #555; margin-top: 20px;">Download BeboCard to manage all your loyalty cards and receipts in one private vault.</p>
                    \`;
                }, 800);
            } catch (err) {
                alert('Connection failed. Please try again.');
                btn.innerText = 'Get Digital Receipt';
                btn.disabled = false;
            }
        }
    </script>
</body>
</html>
  `;

  return {
    statusCode: 200,
    headers: { 
        'Content-Type': 'text/html',
        'Cache-Control': 'public, max-age=3600'
    },
    body: html,
  } as APIGatewayProxyResult;
};
