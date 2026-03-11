from kucoin_futures.client import User

api_key = "69aff75da0f4ba0001be968f"
api_secret = "052aaf01-f999-4d75-a9b4-49ebbbd0087b"
api_passphrase = "08122005_Den"

client = User(api_key, api_secret, api_passphrase)

accounts = client.get_deposit_list()
for acc in accounts:
    balance = float(acc["balance"])
    if balance > 0:
        print(f"{acc['currency']} | Баланс: {acc['balance']} | Доступно: {acc['availableBalance']}")