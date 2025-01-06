# Apple search ads Oauth2 獲得 Client Secret
import jwt
import datetime as dt

# 變更當前工作目錄
import os
new_directory = r"C:\git\Apple_search_ads_API\ASA_client_secret"
os.chdir(new_directory)

# ASA後台提供的客戶端金鑰資訊
client_id = 'client_id'
team_id = 'team_id' 
key_id = 'key_id' 
audience = 'https://appleid.apple.com'
alg = 'alg'

# 客戶金鑰有效時間
# Define issue timestamp.
issued_at_timestamp = int(dt.datetime.utcnow().timestamp())
# Define expiration timestamp. May not exceed 180 days from issue timestamp.
expiration_timestamp = issued_at_timestamp + 86400*180 

# JWT參數
headers = dict()
headers['alg'] = alg
headers['kid'] = key_id
# Define JWT payload.
payload = dict()
payload['sub'] = client_id
payload['aud'] = audience
payload['iat'] = issued_at_timestamp
payload['exp'] = expiration_timestamp
payload['iss'] = team_id 

# 讀取private key 資訊
KEY_FILE = 'private-key.pem' 
with open(KEY_FILE,'r') as key_file:
     key = ''.join(key_file.readlines())

# 使用JWT獲得客戶端金鑰
client_secret = jwt.encode(
payload=payload,  
headers=headers,
algorithm=alg,  
key=key
)
