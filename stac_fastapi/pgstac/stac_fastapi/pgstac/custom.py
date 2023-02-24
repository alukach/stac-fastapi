import json
from typing import Dict, Union

from asyncpg import pool
from buildpg import render
from fastapi import Request
from fastapi.security import HTTPBasic

from stac_fastapi.pgstac.db import translate_pgstac_errors

auth = HTTPBasic()


async def custom_dbfunc(pool: pool, func: str, arg: Union[str, Dict], request: Request):
    """
    This is our custom dbfunc definition. It shares the same signature as pgSTAC's
    dbfunc but validates user credentials and sets the user within the DB connection
    before running view queries.

    Wrap PLPGSQL Functions.

    Keyword arguments:
    pool -- the asyncpg pool to use to connect to the database
    func -- the name of the PostgreSQL function to call
    arg -- the argument to the PostgreSQL function as either a string
    or a dict that will be converted into jsonb
    request -- the fastapi request associated with the call
    """
    # Require & parse basic auth
    credentials = await auth(request)

    with translate_pgstac_errors():
        async with pool.acquire() as conn:
            # Validate credentials...
            q, p = render(
                f"""
                SELECT authenticate_user(:username::text, :password::text)
                """,
                username=credentials.username,
                password=credentials.password,
            )
            await conn.execute(q, *p)

            # Set user in DB...
            q, p = render(
                f"""
                SELECT set_config('api.username', :username::text, false)
                """,
                username=credentials.username,
            )
            await conn.execute(q, *p)

            # Run view query...
            cast_type = "text" if isinstance(arg, str) else "text::jsonb"
            q, p = render(
                f"""
                SELECT * FROM {func}(:item::{cast_type});
                """,
                item=arg if isinstance(arg, str) else json.dumps(arg),
            )
            return await conn.fetchval(q, *p)
