from flask import Blueprint, render_template, request, redirect, session, flash, url_for
import os

import psycopg
from psycopg.rows import tuple_row

from werkzeug.security import check_password_hash

login_bp = Blueprint('login_bp', __name__)

# --------------------------------------------------------------------
# Centralize DB connection (Render Postgres via DATABASE_URL)
# --------------------------------------------------------------------
def get_connection():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    # tuple_row keeps fetchone()/fetchall() as tuples like before
    return psycopg.connect(db_url, row_factory=tuple_row)

@login_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''

        if not username or not password:
            flash('Please fill in both fields.')
            return render_template('login.html')

        conn = get_connection()
        try:
            c = conn.cursor()
            c.execute(
                'SELECT id, password FROM users WHERE name = %s',
                (username,)
            )
            row = c.fetchone()
        finally:
            conn.close()

        if row and check_password_hash(row[1], password):
            session['user_id'] = row[0]
            return redirect(url_for('index'))

        flash('Invalid credentials.')
        return render_template('login.html')

    return render_template('login.html')
