from flask import Blueprint, render_template, request, redirect, url_for, session, flash
import os
import datetime

import psycopg
from psycopg.rows import tuple_row

from werkzeug.security import generate_password_hash

register_bp = Blueprint('register_bp', __name__)

# --------------------------------------------------------------------
# Centralize DB connection (Render Postgres via DATABASE_URL)
# --------------------------------------------------------------------
def get_connection():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg.connect(db_url, row_factory=tuple_row)

# --------------------------------------------------------------------
# Helpers: safe parsing for integer fields coming from HTML forms
# --------------------------------------------------------------------
_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12
}

_VALID_COUNTRIES = {
    "United States",
    "Canada",
    "Mexico",
    "Central America",
    "South America"
}

def _to_int_or_none(v):
    try:
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        return int(s)
    except Exception:
        return None

def _due_month_to_int_or_none(v):
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None

    # Accept numeric months
    mi = _to_int_or_none(s)
    if mi is not None and 1 <= mi <= 12:
        return mi

    # Accept month names like "January"
    key = s.lower()
    return _MONTH_MAP.get(key)

@register_bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        email = (request.form.get('email') or '').strip().lower()
        password = request.form.get('password') or ''
        country = (request.form.get('country') or '').strip()

        year_of_birth_raw = request.form.get('year_of_birth')
        due_month_raw = request.form.get('due_month')

        year_of_birth = _to_int_or_none(year_of_birth_raw)
        due_month = _due_month_to_int_or_none(due_month_raw)

        if email == '':
            email = None

        if not username or not password or not email or not country:
            flash('Por favor complete todos los campos obligatorios.')
            return render_template('register.html')

        if country not in _VALID_COUNTRIES:
            flash('Por favor seleccione un país o región válido.')
            return render_template('register.html')

        hashed_password = generate_password_hash(password)

        conn = get_connection()
        try:
            c = conn.cursor()

            # 1. Create user (Postgres: use RETURNING to get the new id)
            c.execute("""
                INSERT INTO users (name, email, password, year_of_birth, due_month, country)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (username, email, hashed_password, year_of_birth, due_month, country))

            row = c.fetchone()
            user_id = row[0] if row else None
            if not user_id:
                raise RuntimeError("No se pudo crear el usuario (no se devolvió un id)")

            # 2. Attach most recent anonymous assessment (if any)
            anon_id = session.get('anon_id')
            if anon_id:
                c.execute("""
                    UPDATE assessments
                    SET user_id = %s, anon_id = NULL
                    WHERE id = (
                        SELECT id
                        FROM assessments
                        WHERE anon_id = %s
                        ORDER BY created_at DESC
                        LIMIT 1
                    )
                """, (user_id, anon_id))

                session.pop('anon_id', None)

            conn.commit()

            # 3. Log user in and go straight to dashboard
            session['user_id'] = user_id
            flash('Cuenta creada. Sus resultados han sido guardados.')
            return redirect(url_for('dashboard'))

        except psycopg.errors.UniqueViolation:
            conn.rollback()
            flash('Ese nombre de usuario o correo electrónico ya está registrado.')
            return redirect(url_for('register_bp.register'))

        finally:
            conn.close()

    return render_template('register.html')
