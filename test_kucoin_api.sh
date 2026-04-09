curl -s "https://api-futures.kucoin.com/api/v1/contracts/active" | jq '.data[] | select(.symbol=="ARIAUSDTM")'
