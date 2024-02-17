-- Database: rinha

-- DROP DATABASE IF EXISTS rinha;

CREATE DATABASE rinha
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'Portuguese_Brazil.1252'
    LC_CTYPE = 'Portuguese_Brazil.1252'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

GRANT TEMPORARY, CONNECT ON DATABASE rinha TO PUBLIC;

GRANT ALL ON DATABASE rinha TO postgres;

CREATE UNLOGGED TABLE clientes (
	id SERIAL PRIMARY KEY,
	nome VARCHAR(50) NOT NULL,
	limite INTEGER NOT NULL
);

CREATE UNLOGGED TABLE transacoes (
	id SERIAL PRIMARY KEY,
	cliente_id INTEGER NOT NULL,
	valor INTEGER NOT NULL,
	tipo CHAR(1) NOT NULL,
	descricao VARCHAR(10) NOT NULL,
	realizada_em TIMESTAMP NOT NULL DEFAULT NOW(),
	CONSTRAINT fk_clientes_transacoes_id
		FOREIGN KEY (cliente_id) REFERENCES clientes(id)
);

CREATE UNLOGGED TABLE saldos (
	id SERIAL PRIMARY KEY,
	cliente_id INTEGER NOT NULL,
	valor INTEGER NOT NULL,
	CONSTRAINT fk_clientes_saldos_id
		FOREIGN KEY (cliente_id) REFERENCES clientes(id)
);

CREATE INDEX IF NOT EXISTS idx_created
    ON public.transacoes USING btree
    (realizada_em DESC NULLS FIRST)
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;

DO $$
BEGIN
	INSERT INTO clientes (nome, limite)
	VALUES
		('ze', 100000),
		('joao', 300000),
		('peter', 500000),
		('mariza', 700000),
		('quijumo', 900000);

	INSERT INTO saldos (cliente_id, valor)
		SELECT id, 0 FROM clientes;
END;
$$;

CREATE OR REPLACE FUNCTION debitar(
	cliente_id_tx INT,
	valor_tx INT,
	descricao_tx VARCHAR(10))
RETURNS TABLE (
	novo_saldo INT,
	limite INT,
	possui_erro BOOL,
	mensagem VARCHAR(20))
LANGUAGE plpgsql
AS $$
DECLARE
	saldo_atual int;
	limite_atual int;
BEGIN
    -- Creates a lock with client_id_tx, so any other transaction with
    -- the same id will fail
	SELECT pg_advisory_xact_lock(cliente_id_tx);
	SELECT
		c.limite,
		COALESCE(s.valor, 0)
	INTO
		limite_atual,
		saldo_atual
	FROM clientes c
		LEFT JOIN saldos s
			ON c.id = s.cliente_id
	WHERE c.id = cliente_id_tx;

	IF saldo_atual - valor_tx >= limite_atual * -1 THEN
		INSERT INTO transacoes
			VALUES(DEFAULT, cliente_id_tx, valor_tx, 'd', descricao_tx, NOW());

		UPDATE saldos
		SET valor = valor - valor_tx
		WHERE cliente_id = cliente_id_tx;

		RETURN QUERY
			SELECT
				valor as saldo,
				limite_atual,
				FALSE,
				'ok'::VARCHAR(20)
			FROM saldos
			WHERE cliente_id = cliente_id_tx;
	ELSE
		RETURN QUERY
			SELECT
				valor,
				0,
				TRUE,
				'saldo insuficente'::VARCHAR(20)
			FROM saldos
			WHERE cliente_id = cliente_id_tx;
	END IF;
END;
$$;

CREATE OR REPLACE FUNCTION creditar(
	cliente_id_tx INT,
	valor_tx INT,
	descricao_tx VARCHAR(10))
RETURNS TABLE (
	novo_saldo INT,
	limite INT,
	possui_erro BOOL,
	mensagem VARCHAR(20))
LANGUAGE plpgsql
AS $$
DECLARE
	limite_atual int;
BEGIN
	PERFORM pg_advisory_xact_lock(cliente_id_tx);
	SELECT
		c.limite
	INTO
		limite_atual
	FROM clientes c
	WHERE c.id = cliente_id_tx;

	INSERT INTO transacoes
		VALUES(DEFAULT, cliente_id_tx, valor_tx, 'c', descricao_tx, NOW());

	RETURN QUERY
		UPDATE saldos
		SET valor = valor + valor_tx
		WHERE cliente_id = cliente_id_tx
		RETURNING valor, limite_atual, FALSE, 'ok'::VARCHAR(20);
END;
$$;

CREATE OR REPLACE FUNCTION obter_extrato(cliente_id_tx INTEGER)
RETURNS JSON AS $$
DECLARE
    extrato JSON;
BEGIN
    SELECT json_build_object(
        'saldo', json_build_object(
            'total', (SELECT valor FROM saldos WHERE cliente_id = cliente_id_tx),
            'data_extrato', NOW(),
            'limite', (SELECT limite FROM clientes WHERE id = cliente_id_tx)
        ),
        'ultimas_transacoes', COALESCE((
            SELECT json_agg(json_build_object(
                'valor', t.valor,
                'tipo', t.tipo,
                'descricao', t.descricao,
                'realizada_em', t.realizada_em
            ) )
            FROM (SELECT t.valor, t.tipo, t.descricao, t.realizada_em FROM transacoes as t
            WHERE t.cliente_id = cliente_id_tx ORDER BY t.realizada_em DESC
            LIMIT 10) as t
        ), '[]'::JSON)
    )
    INTO extrato;

    RETURN extrato;
END;
$$ LANGUAGE plpgsql;