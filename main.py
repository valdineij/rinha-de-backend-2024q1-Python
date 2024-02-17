from fastapi import FastAPI, Response, status, Query, Path
from typing import Annotated
from connect import transaction, balance

app = FastAPI()

@app.get('/')
async def ready():
    return ({"pai":"on"})

@app.post("/clientes/{id}/transacoes", status_code=200)
async def post_transaction(id,
                           valor,
                           tipo,
                           descricao,
                           response: Response):

    saldo, limite, msg = transaction(id,valor,descricao,tipo)

    if msg=='i':
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"msg": "Cliente inexiste!"}
    elif msg!='ok':
        response.status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
        return {"msg": "Saldo insuficiente!"}
    return {"limite": limite, "saldo": saldo}

@app.get("/clientes/{id}/extrato")
async def get_extrato(id: Annotated[int, Path(ge=1)], response: Response):
    msg = balance(id)
    if msg['saldo']['total']==None:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"msg": "Cliente inexiste!"}
    return msg