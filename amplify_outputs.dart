const amplifyConfig = r'''{
  "auth": {
    "user_pool_id": "us-east-1_y1B0zwCuL",
    "aws_region": "us-east-1",
    "user_pool_client_id": "3o7mhmvca13curtnthvn08hglc",
    "identity_pool_id": "us-east-1:18e15513-23ca-4ddb-bf37-dd037617b1d1",
    "mfa_methods": [
      "TOTP"
    ],
    "standard_required_attributes": [
      "email"
    ],
    "username_attributes": [
      "email"
    ],
    "user_verification_types": [
      "email"
    ],
    "groups": [],
    "mfa_configuration": "OPTIONAL",
    "password_policy": {
      "min_length": 12,
      "require_lowercase": true,
      "require_numbers": true,
      "require_symbols": true,
      "require_uppercase": true
    },
    "unauthenticated_identities_enabled": true
  },
  "data": {
    "url": "https://5vfymave5remnmz3xdaptsnlua.appsync-api.us-east-1.amazonaws.com/graphql",
    "aws_region": "us-east-1",
    "api_key": "da2-q2dijsdonzao7agd62ttv62xhi",
    "default_authorization_type": "AMAZON_COGNITO_USER_POOLS",
    "authorization_types": [
      "API_KEY",
      "AWS_IAM"
    ],
    "model_introspection": {
      "version": 1,
      "models": {
        "UserDataEvent": {
          "name": "UserDataEvent",
          "fields": {
            "pK": {
              "name": "pK",
              "isArray": false,
              "type": "String",
              "isRequired": true,
              "attributes": []
            },
            "sK": {
              "name": "sK",
              "isArray": false,
              "type": "String",
              "isRequired": true,
              "attributes": []
            },
            "eventType": {
              "name": "eventType",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "status": {
              "name": "status",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "primaryCat": {
              "name": "primaryCat",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "subCategory": {
              "name": "subCategory",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "desc": {
              "name": "desc",
              "isArray": false,
              "type": "AWSJSON",
              "isRequired": false,
              "attributes": []
            },
            "secondaryULID": {
              "name": "secondaryULID",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "rotatesAt": {
              "name": "rotatesAt",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "createdAt": {
              "name": "createdAt",
              "isArray": false,
              "type": "AWSDateTime",
              "isRequired": false,
              "attributes": []
            },
            "updatedAt": {
              "name": "updatedAt",
              "isArray": false,
              "type": "AWSDateTime",
              "isRequired": false,
              "attributes": []
            }
          },
          "syncable": true,
          "pluralName": "UserDataEvents",
          "attributes": [
            {
              "type": "model",
              "properties": {}
            },
            {
              "type": "key",
              "properties": {
                "fields": [
                  "pK",
                  "sK"
                ]
              }
            },
            {
              "type": "key",
              "properties": {
                "name": "userDataEventsByPrimaryCatAndCreatedAt",
                "queryField": "userDataByCategory",
                "fields": [
                  "primaryCat",
                  "createdAt"
                ]
              }
            },
            {
              "type": "key",
              "properties": {
                "name": "userDataEventsBySubCategoryAndCreatedAt",
                "queryField": "userDataBySubCategory",
                "fields": [
                  "subCategory",
                  "createdAt"
                ]
              }
            },
            {
              "type": "auth",
              "properties": {
                "rules": [
                  {
                    "provider": "userPools",
                    "ownerField": "owner",
                    "allow": "owner",
                    "identityClaim": "cognito:username",
                    "operations": [
                      "create",
                      "update",
                      "delete",
                      "read"
                    ]
                  }
                ]
              }
            }
          ],
          "primaryKeyInfo": {
            "isCustomPrimaryKey": true,
            "primaryKeyFieldName": "pK",
            "sortKeyFieldNames": [
              "sK"
            ]
          }
        },
        "ReportDataEvent": {
          "name": "ReportDataEvent",
          "fields": {
            "pK": {
              "name": "pK",
              "isArray": false,
              "type": "String",
              "isRequired": true,
              "attributes": []
            },
            "sK": {
              "name": "sK",
              "isArray": false,
              "type": "String",
              "isRequired": true,
              "attributes": []
            },
            "eventType": {
              "name": "eventType",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "status": {
              "name": "status",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "desc": {
              "name": "desc",
              "isArray": false,
              "type": "AWSJSON",
              "isRequired": false,
              "attributes": []
            },
            "createdAt": {
              "name": "createdAt",
              "isArray": false,
              "type": "AWSDateTime",
              "isRequired": false,
              "attributes": []
            },
            "updatedAt": {
              "name": "updatedAt",
              "isArray": false,
              "type": "AWSDateTime",
              "isRequired": false,
              "attributes": []
            }
          },
          "syncable": true,
          "pluralName": "ReportDataEvents",
          "attributes": [
            {
              "type": "model",
              "properties": {}
            },
            {
              "type": "key",
              "properties": {
                "fields": [
                  "pK",
                  "sK"
                ]
              }
            },
            {
              "type": "auth",
              "properties": {
                "rules": [
                  {
                    "groupClaim": "cognito:groups",
                    "provider": "userPools",
                    "allow": "groups",
                    "groups": [
                      "admin"
                    ],
                    "operations": [
                      "create",
                      "update",
                      "delete",
                      "read"
                    ]
                  }
                ]
              }
            }
          ],
          "primaryKeyInfo": {
            "isCustomPrimaryKey": true,
            "primaryKeyFieldName": "pK",
            "sortKeyFieldNames": [
              "sK"
            ]
          }
        },
        "RefDataEvent": {
          "name": "RefDataEvent",
          "fields": {
            "pK": {
              "name": "pK",
              "isArray": false,
              "type": "String",
              "isRequired": true,
              "attributes": []
            },
            "sK": {
              "name": "sK",
              "isArray": false,
              "type": "String",
              "isRequired": true,
              "attributes": []
            },
            "eventType": {
              "name": "eventType",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "status": {
              "name": "status",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "primaryCat": {
              "name": "primaryCat",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "subCategory": {
              "name": "subCategory",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "desc": {
              "name": "desc",
              "isArray": false,
              "type": "AWSJSON",
              "isRequired": false,
              "attributes": []
            },
            "createdAt": {
              "name": "createdAt",
              "isArray": false,
              "type": "AWSDateTime",
              "isRequired": false,
              "attributes": []
            },
            "updatedAt": {
              "name": "updatedAt",
              "isArray": false,
              "type": "AWSDateTime",
              "isRequired": false,
              "attributes": []
            },
            "version": {
              "name": "version",
              "isArray": false,
              "type": "Int",
              "isRequired": false,
              "attributes": []
            },
            "tenantId": {
              "name": "tenantId",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "brandId": {
              "name": "brandId",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "roleKey": {
              "name": "roleKey",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "subjectEmail": {
              "name": "subjectEmail",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "keyId": {
              "name": "keyId",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "logoUrl": {
              "name": "logoUrl",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "bannerUrl": {
              "name": "bannerUrl",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "offerImageUrl": {
              "name": "offerImageUrl",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            }
          },
          "syncable": true,
          "pluralName": "RefDataEvents",
          "attributes": [
            {
              "type": "model",
              "properties": {}
            },
            {
              "type": "key",
              "properties": {
                "fields": [
                  "pK",
                  "sK"
                ]
              }
            },
            {
              "type": "key",
              "properties": {
                "name": "refDataEventsByStatusAndPrimaryCat",
                "queryField": "refDataByStatus",
                "fields": [
                  "status",
                  "primaryCat"
                ]
              }
            },
            {
              "type": "key",
              "properties": {
                "name": "refDataEventsByPrimaryCatAndSubCategory",
                "queryField": "refDataByCategory",
                "fields": [
                  "primaryCat",
                  "subCategory"
                ]
              }
            },
            {
              "type": "key",
              "properties": {
                "name": "refDataEventsByTenantIdAndSK",
                "queryField": "refDataByTenant",
                "fields": [
                  "tenantId",
                  "sK"
                ]
              }
            },
            {
              "type": "key",
              "properties": {
                "name": "refDataEventsByBrandId",
                "queryField": "refDataByBrand",
                "fields": [
                  "brandId"
                ]
              }
            },
            {
              "type": "key",
              "properties": {
                "name": "refDataEventsBySubjectEmailAndTenantId",
                "queryField": "refDataBySubjectEmail",
                "fields": [
                  "subjectEmail",
                  "tenantId"
                ]
              }
            },
            {
              "type": "key",
              "properties": {
                "name": "refDataEventsByKeyId",
                "queryField": "refDataByKeyId",
                "fields": [
                  "keyId"
                ]
              }
            },
            {
              "type": "auth",
              "properties": {
                "rules": [
                  {
                    "allow": "private",
                    "operations": [
                      "read"
                    ]
                  },
                  {
                    "groupClaim": "cognito:groups",
                    "provider": "userPools",
                    "allow": "groups",
                    "groups": [
                      "admin"
                    ],
                    "operations": [
                      "create",
                      "update",
                      "delete",
                      "read"
                    ]
                  }
                ]
              }
            }
          ],
          "primaryKeyInfo": {
            "isCustomPrimaryKey": true,
            "primaryKeyFieldName": "pK",
            "sortKeyFieldNames": [
              "sK"
            ]
          }
        },
        "AdminDataEvent": {
          "name": "AdminDataEvent",
          "fields": {
            "pK": {
              "name": "pK",
              "isArray": false,
              "type": "String",
              "isRequired": true,
              "attributes": []
            },
            "sK": {
              "name": "sK",
              "isArray": false,
              "type": "String",
              "isRequired": true,
              "attributes": []
            },
            "eventType": {
              "name": "eventType",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "status": {
              "name": "status",
              "isArray": false,
              "type": "String",
              "isRequired": false,
              "attributes": []
            },
            "desc": {
              "name": "desc",
              "isArray": false,
              "type": "AWSJSON",
              "isRequired": false,
              "attributes": []
            },
            "createdAt": {
              "name": "createdAt",
              "isArray": false,
              "type": "AWSDateTime",
              "isRequired": false,
              "attributes": []
            },
            "updatedAt": {
              "name": "updatedAt",
              "isArray": false,
              "type": "AWSDateTime",
              "isRequired": false,
              "attributes": []
            }
          },
          "syncable": true,
          "pluralName": "AdminDataEvents",
          "attributes": [
            {
              "type": "model",
              "properties": {}
            },
            {
              "type": "key",
              "properties": {
                "fields": [
                  "pK",
                  "sK"
                ]
              }
            },
            {
              "type": "auth",
              "properties": {
                "rules": [
                  {
                    "groupClaim": "cognito:groups",
                    "provider": "userPools",
                    "allow": "groups",
                    "groups": [
                      "admin"
                    ],
                    "operations": [
                      "create",
                      "update",
                      "delete",
                      "read"
                    ]
                  }
                ]
              }
            }
          ],
          "primaryKeyInfo": {
            "isCustomPrimaryKey": true,
            "primaryKeyFieldName": "pK",
            "sortKeyFieldNames": [
              "sK"
            ]
          }
        }
      },
      "enums": {},
      "nonModels": {},
      "queries": {
        "getStampCard": {
          "name": "getStampCard",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "brandId": {
              "name": "brandId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "listStampCards": {
          "name": "listStampCards",
          "isArray": true,
          "type": "AWSJSON",
          "isRequired": false,
          "isArrayNullable": true
        },
        "getNearbyStores": {
          "name": "getNearbyStores",
          "isArray": true,
          "type": "AWSJSON",
          "isRequired": false,
          "isArrayNullable": true,
          "arguments": {
            "brandId": {
              "name": "brandId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "lat": {
              "name": "lat",
              "isArray": false,
              "type": "Float",
              "isRequired": true
            },
            "lng": {
              "name": "lng",
              "isArray": false,
              "type": "Float",
              "isRequired": true
            },
            "radiusKm": {
              "name": "radiusKm",
              "isArray": false,
              "type": "Float",
              "isRequired": true
            },
            "limit": {
              "name": "limit",
              "isArray": false,
              "type": "Int",
              "isRequired": true
            }
          }
        }
      },
      "mutations": {
        "addLoyaltyCard": {
          "name": "addLoyaltyCard",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "brandId": {
              "name": "brandId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "cardNumber": {
              "name": "cardNumber",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "cardLabel": {
              "name": "cardLabel",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "isCustom": {
              "name": "isCustom",
              "isArray": false,
              "type": "Boolean",
              "isRequired": false
            },
            "customBrandName": {
              "name": "customBrandName",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "customBrandColor": {
              "name": "customBrandColor",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "isDefault": {
              "name": "isDefault",
              "isArray": false,
              "type": "Boolean",
              "isRequired": false
            },
            "barcodeType": {
              "name": "barcodeType",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "storeId": {
              "name": "storeId",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "attributionBrandId": {
              "name": "attributionBrandId",
              "isArray": false,
              "type": "String",
              "isRequired": false
            }
          }
        },
        "removeLoyaltyCard": {
          "name": "removeLoyaltyCard",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "cardSK": {
              "name": "cardSK",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "setDefaultCard": {
          "name": "setDefaultCard",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "cardSK": {
              "name": "cardSK",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "brandId": {
              "name": "brandId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "subscribeToOffers": {
          "name": "subscribeToOffers",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "brandId": {
              "name": "brandId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "unsubscribeFromOffers": {
          "name": "unsubscribeFromOffers",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "brandId": {
              "name": "brandId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "updateIdentity": {
          "name": "updateIdentity",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "globalSnoozeStart": {
              "name": "globalSnoozeStart",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "globalSnoozeEnd": {
              "name": "globalSnoozeEnd",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "lastActiveHour": {
              "name": "lastActiveHour",
              "isArray": false,
              "type": "Int",
              "isRequired": false
            },
            "displayName": {
              "name": "displayName",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "email": {
              "name": "email",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "phone": {
              "name": "phone",
              "isArray": false,
              "type": "String",
              "isRequired": false
            }
          }
        },
        "rotateQR": {
          "name": "rotateQR",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false
        },
        "getOrRefreshIdentity": {
          "name": "getOrRefreshIdentity",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false
        },
        "addGiftCard": {
          "name": "addGiftCard",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "brandName": {
              "name": "brandName",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "brandColor": {
              "name": "brandColor",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "cardNumber": {
              "name": "cardNumber",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "cardLabel": {
              "name": "cardLabel",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "balance": {
              "name": "balance",
              "isArray": false,
              "type": "Float",
              "isRequired": false
            },
            "currency": {
              "name": "currency",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "expiryDate": {
              "name": "expiryDate",
              "isArray": false,
              "type": "String",
              "isRequired": false
            }
          }
        },
        "removeGiftCard": {
          "name": "removeGiftCard",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "cardSK": {
              "name": "cardSK",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "updateGiftCardBalance": {
          "name": "updateGiftCardBalance",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "cardSK": {
              "name": "cardSK",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "balance": {
              "name": "balance",
              "isArray": false,
              "type": "Float",
              "isRequired": true
            }
          }
        },
        "addInvoice": {
          "name": "addInvoice",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "supplier": {
              "name": "supplier",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "amount": {
              "name": "amount",
              "isArray": false,
              "type": "Float",
              "isRequired": true
            },
            "dueDate": {
              "name": "dueDate",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "invoiceNumber": {
              "name": "invoiceNumber",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "category": {
              "name": "category",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "notes": {
              "name": "notes",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "currency": {
              "name": "currency",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "brandId": {
              "name": "brandId",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "linkedSubscriptionSk": {
              "name": "linkedSubscriptionSk",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "providerId": {
              "name": "providerId",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "billingPeriod": {
              "name": "billingPeriod",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "invoiceType": {
              "name": "invoiceType",
              "isArray": false,
              "type": "String",
              "isRequired": false
            }
          }
        },
        "updateInvoiceStatus": {
          "name": "updateInvoiceStatus",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "invoiceSK": {
              "name": "invoiceSK",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "status": {
              "name": "status",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "paidDate": {
              "name": "paidDate",
              "isArray": false,
              "type": "String",
              "isRequired": false
            }
          }
        },
        "removeInvoice": {
          "name": "removeInvoice",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "invoiceSK": {
              "name": "invoiceSK",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "addReceipt": {
          "name": "addReceipt",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "merchant": {
              "name": "merchant",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "amount": {
              "name": "amount",
              "isArray": false,
              "type": "Float",
              "isRequired": true
            },
            "purchaseDate": {
              "name": "purchaseDate",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "category": {
              "name": "category",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "notes": {
              "name": "notes",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "warrantyExpiry": {
              "name": "warrantyExpiry",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "items": {
              "name": "items",
              "isArray": false,
              "type": "AWSJSON",
              "isRequired": false
            },
            "photoKey": {
              "name": "photoKey",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "loyaltyCardSK": {
              "name": "loyaltyCardSK",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "currency": {
              "name": "currency",
              "isArray": false,
              "type": "String",
              "isRequired": false
            }
          }
        },
        "removeReceipt": {
          "name": "removeReceipt",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "receiptSK": {
              "name": "receiptSK",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "markNewsletterRead": {
          "name": "markNewsletterRead",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "newsletterSK": {
              "name": "newsletterSK",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "updateSubscription": {
          "name": "updateSubscription",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "brandId": {
              "name": "brandId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "offers": {
              "name": "offers",
              "isArray": false,
              "type": "Boolean",
              "isRequired": false
            },
            "newsletters": {
              "name": "newsletters",
              "isArray": false,
              "type": "Boolean",
              "isRequired": false
            },
            "reminders": {
              "name": "reminders",
              "isArray": false,
              "type": "Boolean",
              "isRequired": false
            },
            "catalogues": {
              "name": "catalogues",
              "isArray": false,
              "type": "Boolean",
              "isRequired": false
            }
          }
        },
        "snoozeOffers": {
          "name": "snoozeOffers",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "brandId": {
              "name": "brandId",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "until": {
              "name": "until",
              "isArray": false,
              "type": "String",
              "isRequired": false
            }
          }
        },
        "updatePreferences": {
          "name": "updatePreferences",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "reminders": {
              "name": "reminders",
              "isArray": false,
              "type": "AWSJSON",
              "isRequired": true
            }
          }
        },
        "respondToCheckout": {
          "name": "respondToCheckout",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "orderId": {
              "name": "orderId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "approved": {
              "name": "approved",
              "isArray": false,
              "type": "Boolean",
              "isRequired": true
            },
            "paymentToken": {
              "name": "paymentToken",
              "isArray": false,
              "type": "String",
              "isRequired": false
            }
          }
        },
        "cancelRecurring": {
          "name": "cancelRecurring",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "subId": {
              "name": "subId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "brandId": {
              "name": "brandId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "addManualSubscription": {
          "name": "addManualSubscription",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "brandName": {
              "name": "brandName",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "productName": {
              "name": "productName",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "amount": {
              "name": "amount",
              "isArray": false,
              "type": "Float",
              "isRequired": true
            },
            "currency": {
              "name": "currency",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "frequency": {
              "name": "frequency",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "nextBillingDate": {
              "name": "nextBillingDate",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "category": {
              "name": "category",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "respondToConsent": {
          "name": "respondToConsent",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "requestId": {
              "name": "requestId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "approvedFields": {
              "name": "approvedFields",
              "isArray": true,
              "type": "String",
              "isRequired": false,
              "isArrayNullable": false
            }
          }
        },
        "setRotationFrequency": {
          "name": "setRotationFrequency",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "frequency": {
              "name": "frequency",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "respondToEnrollment": {
          "name": "respondToEnrollment",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "enrollmentId": {
              "name": "enrollmentId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "accepted": {
              "name": "accepted",
              "isArray": false,
              "type": "Boolean",
              "isRequired": true
            }
          }
        },
        "initiateEnrollment": {
          "name": "initiateEnrollment",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "brandId": {
              "name": "brandId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "purchaseGiftCard": {
          "name": "purchaseGiftCard",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "brandId": {
              "name": "brandId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "catalogItemId": {
              "name": "catalogItemId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "denomination": {
              "name": "denomination",
              "isArray": false,
              "type": "Float",
              "isRequired": true
            },
            "currency": {
              "name": "currency",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "syncGiftCardBalance": {
          "name": "syncGiftCardBalance",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "cardSK": {
              "name": "cardSK",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "brandId": {
              "name": "brandId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "purchaseForSelf": {
          "name": "purchaseForSelf",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "brandId": {
              "name": "brandId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "skuId": {
              "name": "skuId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "denomination": {
              "name": "denomination",
              "isArray": false,
              "type": "Float",
              "isRequired": true
            },
            "currency": {
              "name": "currency",
              "isArray": false,
              "type": "String",
              "isRequired": false
            }
          }
        },
        "purchaseAsGift": {
          "name": "purchaseAsGift",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "brandId": {
              "name": "brandId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "skuId": {
              "name": "skuId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "denomination": {
              "name": "denomination",
              "isArray": false,
              "type": "Float",
              "isRequired": true
            },
            "currency": {
              "name": "currency",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "recipientEmail": {
              "name": "recipientEmail",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "senderDisplayName": {
              "name": "senderDisplayName",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "message": {
              "name": "message",
              "isArray": false,
              "type": "String",
              "isRequired": false
            }
          }
        },
        "reportGeofenceEntry": {
          "name": "reportGeofenceEntry",
          "isArray": false,
          "type": "String",
          "isRequired": false,
          "arguments": {
            "secondaryULID": {
              "name": "secondaryULID",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "geofenceId": {
              "name": "geofenceId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "entryTime": {
              "name": "entryTime",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "registerDeviceToken": {
          "name": "registerDeviceToken",
          "isArray": false,
          "type": "String",
          "isRequired": false,
          "arguments": {
            "token": {
              "name": "token",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "platform": {
              "name": "platform",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "permULID": {
              "name": "permULID",
              "isArray": false,
              "type": "String",
              "isRequired": false
            }
          }
        },
        "unregisterDeviceToken": {
          "name": "unregisterDeviceToken",
          "isArray": false,
          "type": "String",
          "isRequired": false,
          "arguments": {
            "token": {
              "name": "token",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "permULID": {
              "name": "permULID",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "recordBipaConsent": {
          "name": "recordBipaConsent",
          "isArray": false,
          "type": "Boolean",
          "isRequired": false,
          "arguments": {
            "textVersion": {
              "name": "textVersion",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        },
        "startDataExport": {
          "name": "startDataExport",
          "isArray": false,
          "type": "String",
          "isRequired": false
        },
        "deleteUserAccount": {
          "name": "deleteUserAccount",
          "isArray": false,
          "type": "String",
          "isRequired": false
        },
        "trackEngagement": {
          "name": "trackEngagement",
          "isArray": false,
          "type": "AWSJSON",
          "isRequired": false,
          "arguments": {
            "eventType": {
              "name": "eventType",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "targetId": {
              "name": "targetId",
              "isArray": false,
              "type": "String",
              "isRequired": true
            },
            "source": {
              "name": "source",
              "isArray": false,
              "type": "String",
              "isRequired": false
            },
            "metadata": {
              "name": "metadata",
              "isArray": false,
              "type": "AWSJSON",
              "isRequired": false
            },
            "permULID": {
              "name": "permULID",
              "isArray": false,
              "type": "String",
              "isRequired": true
            }
          }
        }
      }
    }
  },
  "version": "1.4"
}''';