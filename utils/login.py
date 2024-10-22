'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

# Initialize Keycloak OpenID client
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError
from keycloak.exceptions import KeycloakPostError
from keycloak.keycloak_openid import KeycloakOpenID
from pydantic import BaseModel
from starlette import status as return_status

from configs import config

keycloak_openid = KeycloakOpenID(server_url=config.KEYCLOAK_SERVER_URL,
                                 client_id=config.KEYCLOAK_CLIENT_ID,
                                 realm_name=config.KEYCLOAK_REALM_NAME)

http_bearer = HTTPBearer()


class TokenData(BaseModel):
    """Token class"""
    username: str


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(http_bearer)):
    """Function to verify token"""
    try:
        token = credentials.credentials
        userinfo = keycloak_openid.userinfo(token)
        username: str = userinfo.get("preferred_username")
        if username is None:
            raise HTTPException(
                status_code=return_status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )

        token_data = TokenData(username=username)

    except JWTError:
        raise HTTPException(
            status_code=return_status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    except KeycloakPostError as e:
        raise HTTPException(
            status_code=return_status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Authentication Token, please Re-login",
            headers={"WWW-Authenticate": "Bearer"},
        )

    except Exception as e:
        raise HTTPException(
            status_code=return_status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return token_data
