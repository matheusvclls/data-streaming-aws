import os
import json
import random
import boto3
from botocore.config import Config
import datetime as dt

# =========================
# Configurações
# =========================
DELIVERY_STREAM_NAME = os.getenv("FIREHOSE_STREAM", "firehose-orders")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

TOTAL = int(os.getenv("TOTAL", "10000"))       # total de registros
BATCH_SIZE = int(os.getenv("BATCH", "100"))    # tamanho do lote para o put_record_batch
P_UPDATE = float(os.getenv("P_UPDATE", "0.30"))  # probabilidade de gerar um "update"

# Cliente Firehose
fh = boto3.client("firehose", region_name=AWS_REGION,
                  config=Config(retries={"max_attempts": 10}))

# Métodos de pagamento (inclui opções comuns no BR)
PAYMENT_METHODS = ["credit_card", "debit_card", "pix", "boleto", "paypal"]

NUM_PRODUCTS = 500  # variedade de produtos simulados

# Estado em memória para permitir updates consistentes
_state = {
    "next_order_id": 1,
    "orders": {}  # order_id -> {"user_id", "product_id", "amount", "payment_method"}
}

def _mk_order(p_update: float = 0.30) -> dict:
    """
    Gera um registro de ordem.
    - 'i' (insert): cria uma nova ordem com order_id sequencial.
    - 'u' (update): escolhe uma ordem existente e ajusta alguns campos (ex.: amount).
    Retorna somente as colunas solicitadas.
    """
    has_orders = bool(_state["orders"])
    do_update = has_orders and (random.random() < p_update)

    if do_update:
        op = "u"
        order_id = random.choice(list(_state["orders"].keys()))
        o = _state["orders"][order_id]

        # Atualiza levemente o amount e, às vezes, o payment_method
        new_amount = max(1.0, round(o["amount"] * random.uniform(0.8, 1.2), 2))
        new_payment = random.choice(PAYMENT_METHODS) if random.random() < 0.15 else o["payment_method"]

        # Atualiza estado
        o["amount"] = new_amount
        o["payment_method"] = new_payment

        user_id = o["user_id"]
        product_id = o["product_id"]
        amount = new_amount
        payment_method = new_payment
    else:
        op = "i"
        order_id = _state["next_order_id"]
        _state["next_order_id"] += 1

        user_id = f"{random.randint(1, 1000)}"
        product_id = f"{random.randint(1, NUM_PRODUCTS)}"
        amount = round(random.uniform(5.0, 800.0), 2)
        payment_method = random.choice(PAYMENT_METHODS)

        _state["orders"][order_id] = {
            "user_id": user_id,
            "product_id": product_id,
            "amount": amount,
            "payment_method": payment_method
        }

    # Apenas as colunas solicitadas
    now = dt.datetime.utcnow()
    return {
        "order_id": order_id,
        "user_id": user_id,
        "product_id": product_id,
        "database_operation": op,
        "amount": amount,
        "payment_method": payment_method
    }

def test_locally(total: int = TOTAL, batch_size: int = BATCH_SIZE, p_update: float = P_UPDATE):
    buf = []
    for _ in range(total):
        rec = _mk_order(p_update=p_update)
        print(rec)
        buf.append({"Data": (json.dumps(rec) + "\n").encode("utf-8")})


def put_batch_orders(total: int = TOTAL, batch_size: int = BATCH_SIZE, p_update: float = P_UPDATE):
    buf = []
    for _ in range(total):
        rec = _mk_order(p_update=p_update)
        print(rec)
        buf.append({"Data": (json.dumps(rec) + "\n").encode("utf-8")})

        if len(buf) >= batch_size:
            fh.put_record_batch(DeliveryStreamName=DELIVERY_STREAM_NAME, Records=buf)
            buf.clear()

    if buf:
        fh.put_record_batch(DeliveryStreamName=DELIVERY_STREAM_NAME, Records=buf)

if __name__ == "__main__":
    #put_batch_orders()
    test_locally()
    print("done")
