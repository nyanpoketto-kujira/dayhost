"""Login / logout. HTML form + cookie session."""
from __future__ import annotations

from fastapi import APIRouter, Form, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse

from .. import auth, db
from ..config import get_settings
from ..templating import templates

router = APIRouter(tags=["auth"])


@router.get("/login", response_class=HTMLResponse)
async def login_form(request: Request) -> HTMLResponse:
    settings = get_settings()
    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "title": "Sign in",
            "auth_configured": settings.auth_configured,
        },
    )


@router.post("/login")
async def login_submit(
    request: Request,
    username: str = Form(""),
    password: str = Form(""),
) -> RedirectResponse:
    settings = get_settings()
    valid = (
        settings.auth_configured
        and username.strip() == settings.admin_user
        and auth.verify_password(password, settings.admin_password_hash)
    )
    if not valid:
        await db.log_event(f"failed login for {username!r}", level="warn", source="auth")
        return templates.TemplateResponse(
            request,
            "login.html",
            {
                "title": "Sign in",
                "error": "Invalid username or password.",
                "auth_configured": settings.auth_configured,
            },
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    token, expires_at = await auth.create_session(settings.admin_user, settings)
    response = RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    response.set_cookie(
        value=token,
        max_age=settings.session_ttl,
        **{k: v for k, v in auth.session_cookie_params(settings, settings.session_ttl).items() if k != "max_age"},
    )
    return response


@router.post("/logout")
async def logout(request: Request) -> RedirectResponse:
    settings = get_settings()
    token = request.cookies.get(settings.session_cookie, "")
    if token:
        await auth.destroy_session(token, settings)
    response = RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)
    response.delete_cookie(settings.session_cookie, path="/")
    return response
