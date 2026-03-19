from flask import Flask, request, render_template, redirect, session, url_for, jsonify, flash, send_from_directory
from python_modules.login_module import login_bp
from python_modules.register_module import register_bp
from python_modules.messaging_deterministic import get_messages, get_next_message
from python_modules.risk_gdm import gdm_lookup
from python_modules.risk_ptb import ptb_lookup
from python_modules.risk_ght import ght_lookup
import json, datetime, os
import uuid
from typing import Optional
from flask import make_response
import httpx

import psycopg
from psycopg.rows import tuple_row

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

TEMPLATE_DIR = os.path.join(BASE_DIR, "..", "templates")
STATIC_DIR = os.path.join(BASE_DIR, "..", "static")

app = Flask(
    __name__,
    template_folder=TEMPLATE_DIR,
    static_folder=STATIC_DIR
)

app.secret_key = 'change-this-to-something-random'

# -------------------------------------------------------------
# Pregnancy Health News feed (served from PythonAnywhere static)
# -------------------------------------------------------------
NEWS_FEED_URL = "https://mgostine.pythonanywhere.com/static/latest_60.json"

@app.before_request
def redirect_naked_domain():
    if request.host == "babyqscore.org":
        return redirect(
            "https://www.babyqscore.org" + request.full_path,
            code=301
        )

app.register_blueprint(login_bp)
app.register_blueprint(register_bp)

# -------------------------------------------------------------
# Cache-Control: Improve speed for returning visitors
# -------------------------------------------------------------
@app.after_request
def add_cache_headers(response):
    """
    Add caching headers to static files for better load speed.
    """
    if request.path.startswith('/static/'):
        response.headers['Cache-Control'] = 'public, max-age=604800'  # cache for 7 days
    return response

# --------------------------------------------------------------------
# Centralize DB connection (Render Postgres via DATABASE_URL)
# --------------------------------------------------------------------
def get_connection():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    # tuple_row preserves tuple fetch behavior similar to pymysql
    return psycopg.connect(db_url, row_factory=tuple_row)

# --------------------------------------------------------------------
# Centralize data table paths (can be overridden via env variables)
# --------------------------------------------------------------------
app.config['GDM_XLSX_PATH'] = os.environ.get(
    "GDM_XLSX_PATH",
    "/home/mgostine/babyqscore/data/GDM_Version_fixed_Row_Restored.xlsx"
)
app.config['PTB_XLSX_PATH'] = os.environ.get(
    "PTB_XLSX_PATH",
    "/home/mgostine/babyqscore/data/PTB_Version_2025_fixed_Row_Restored.xlsx"
)
app.config['GHT_XLSX_PATH'] = os.environ.get(
    "GHT_XLSX_PATH",
    "/home/mgostine/babyqscore/data/GHT_Version_fixed_Row_Restored.xlsx"
)

def init_assessments_table():
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS assessments (
                id INT PRIMARY KEY AUTO_INCREMENT,
                user_id INT NOT NULL,
                created_at DATETIME NOT NULL,
                total_score INT NOT NULL,
                lifestyle INT NOT NULL,
                exercise INT NOT NULL,
                nutrition INT NOT NULL,
                support INT NOT NULL,
                no_questions TEXT NOT NULL
            )
        """)
        conn.commit()
    finally:
        conn.close()

# init_assessments_table()

QUESTIONS = [
    "I have avoided drinking any alcohol during my pregnancy.",                  #1
    "I have not smoked during my pregnancy.",                                   #2
    "I get 7 to 9 hours of restful sleep most nights during my pregnancy.",     #3
    "I attend all scheduled prenatal checkups (or have a plan to attend them).",#4
    "I perform structured exercise (like brisk walking, classes, or prenatal yoga) for at least 20 - 30 minutes per day 4 to 5 days per week.", #5
    "I eat at least two helpings of protein rich foods per day (poultry, eggs, beef, pork, seafood, tofu, tempeh, lentils).",               #6
    "I limit sugary snacks to no more than 30 grams of sugar a day equal to one mini Coke or 3 Toll House cookies.",                       #7
    "I feel mostly hopeful, positive, and able to enjoy daily activities, and rarely feel depressed.",                                     #8
    "I eat 2 to 3 servings of vegetables at least 5 days a week.",                                                                          #9
    "I have adequate support from my husband or partner, family and friends.",                                                              #10
    "I manage stress and anxiety well and rarely feel overwhelmed.",                                                                         #11
    "I have had a dental checkup and cleaning within the past 6 months or have one scheduled during my pregnancy.",                         #12
    "I take a daily prenatal vitamin that contains at least 400 micrograms (mcg) of folic acid to support my baby's health.",               #13
    "I consume at least one serving of dairy daily (such as milk, cheese, or yogurt) to support my baby's growth.",                         #14
    "I regularly eat fiber-rich whole grains: barley, oatmeal, corn, nut butter, beans, quinoa, brown rice, bulgur."                        #15
]

POINTS = [19, 19, 6, 4, 14, 4, 4, 6, 4, 4, 2, 2, 4, 4, 4]

LIFESTYLE = {1, 2, 3, 4, 11, 12}
EXERCISE  = {5}
NUTRITION = {6, 7, 9, 13, 14, 15}
SUPPORT   = {8, 10}

@app.route("/index", methods=["GET", "POST"])
def index():
    # Ensure anonymous session ID for non-logged-in users
    if 'user_id' not in session and 'anon_id' not in session:
        session['anon_id'] = uuid.uuid4().hex


    if request.method == "GET":
        session.pop('last_score', None)
        session.pop('last_category_scores', None)
        return render_template("index.html", questions=QUESTIONS)

    answers = []
    total_score = 0
    category_scores = {"Lifestyle": 0, "Exercise": 0, "Nutrition": 0, "Support": 0}
    category_totals = {"Lifestyle": 50, "Exercise": 14, "Nutrition": 20, "Support": 16}
    no_questions = []

    for i in range(len(QUESTIONS)):
        val = request.form.get(f'q{i}')
        is_yes = val == "Yes"
        answers.append(is_yes)
        if is_yes:
            pts = POINTS[i]
            total_score += pts
            q_num = i + 1
            if q_num in LIFESTYLE:
                category_scores["Lifestyle"] += pts
            elif q_num in EXERCISE:
                category_scores["Exercise"] += pts
            elif q_num in NUTRITION:
                category_scores["Nutrition"] += pts
            elif q_num in SUPPORT:
                category_scores["Support"] += pts
        else:
            no_questions.append(i + 1)

    messages = []
    for i, is_yes in enumerate(answers):
        if not is_yes:
            try:
                msgs = get_messages(i + 1) or []
                message_text = msgs[0] if msgs else "No messages available for this question."
            except Exception:
                message_text = "No messages available for this question."
            messages.append({"index": i + 1, "text": message_text})

        session['last_score'] = total_score
    session['last_category_scores'] = category_scores
    session['last_no_questions'] = no_questions

    # ----------------------------------------------------------------
    # Persist assessment to database (Postgres) so dashboard history works
    # ----------------------------------------------------------------
    try:
        user_id_val = session.get('user_id')
        anon_id_val = None
        if not user_id_val:
            anon_id_val = session.get('anon_id')
            if not anon_id_val:
                anon_id_val = uuid.uuid4().hex
                session['anon_id'] = anon_id_val

        created_at = datetime.datetime.utcnow()
        no_json = json.dumps(no_questions)

        conn = get_connection()
        try:
            c = conn.cursor()
            c.execute("""
                INSERT INTO assessments (
                    user_id, anon_id, created_at,
                    total_score, lifestyle, exercise, nutrition, support,
                    no_questions
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                user_id_val, anon_id_val, created_at,
                total_score,
                int(category_scores.get("Lifestyle", 0)),
                int(category_scores.get("Exercise", 0)),
                int(category_scores.get("Nutrition", 0)),
                int(category_scores.get("Support", 0)),
                no_json
            ))
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        # Do not break user flow if DB insert fails; dashboard will fall back to session values.
        print("Assessment insert error:", e)

    return render_template("result.html",
             score=total_score,
             messages=messages,
             category_scores=category_scores,
             category_totals=category_totals)

@app.route("/get_next_message", methods=["POST"])
def get_next_message_route():
    data = request.get_json()
    q_num = data.get("question_number")
    try:
        q_int = int(q_num)
        msg = get_next_message(q_int)
    except Exception:
        msg = "Invalid question number"
    return {"message": msg}

@app.route("/dashboard", methods=["GET"])
def dashboard():
    if 'user_id' not in session:
        return redirect(url_for('login_bp.login'))

    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT created_at, total_score, lifestyle, exercise, nutrition, support, no_questions
        FROM assessments
        WHERE user_id = %s
        ORDER BY created_at DESC
        LIMIT 5
    """, (session['user_id'],))
    rows = c.fetchall()
    conn.close()

    history = []
    current = None
    work_items = []

    if rows:
        for idx, r in enumerate(rows):
            created_at, total_score, ls, ex, nu, sup, no_json = r
            item = {
                "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S") if created_at else "",
                "total_score": total_score,
                "LENS": {"Lifestyle": ls, "Exercise": ex, "Nutrition": nu, "Support": sup},
                "no_questions": json.loads(no_json or "[]")
            }
            if idx == 0:
                current = item
            history.append(item)

        if current and current["no_questions"]:
            for qn in current["no_questions"]:
                work_items.append({
                    "q_num": qn,
                    "text": QUESTIONS[qn - 1]
                })
    else:
        if session.get('last_score') is not None:
            current = {
                "created_at": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "total_score": session['last_score'],
                "LENS": session.get('last_category_scores', {"Lifestyle":0,"Exercise":0,"Nutrition":0,"Support":0}),
                "no_questions": session.get('last_no_questions', [])
            }
            history = [current]
            for qn in current["no_questions"]:
                work_items.append({"q_num": qn, "text": QUESTIONS[qn - 1]})

    return render_template("dashboard.html",
                           current=current,
                           history=history,
                           category_totals={"Lifestyle": 50, "Exercise": 14, "Nutrition": 20, "Support": 16},
                           work_items=work_items)

@app.route("/risk", methods=["GET"])
def risk_form():
    return render_template("risk_form.html")

def _compute_bmi_lb_in(weight_lbs: Optional[float], feet: Optional[int], inches: Optional[int]) -> Optional[float]:
    try:
        if weight_lbs is None or feet is None or inches is None:
            return None
        total_inches = feet * 12 + inches
        if total_inches <= 0:
            return None
        return (weight_lbs / (total_inches ** 2)) * 703.0
    except Exception:
        return None

@app.route("/risk_result", methods=["POST"])
def risk_result():
    form = request.form

    def _ival(name: str) -> Optional[int]:
        try:
            v = form.get(name, "").strip()
            return int(v) if v != "" else None
        except Exception:
            return None

    def _fval(name: str) -> Optional[float]:
        try:
            v = form.get(name, "").strip()
            return float(v) if v != "" else None
        except Exception:
            return None

    # Common inputs
    age = _ival("age")
    ethnicity = (form.get("ethnicity") or "").strip()
    weight_pre = _fval("weight_pre")
    height_feet = _ival("height_feet")
    height_inches = _ival("height_inches")
    bmi = _compute_bmi_lb_in(weight_pre, height_feet, height_inches)

    prior_births = _ival("prior_births")
    weeks_pregnant = _ival("weeks_pregnant")
    preg_interval = (form.get("preg_interval") or "").strip()  # '4_11', '12_plus', or ''

    # Medical / history
    hx_preterm = bool(form.get("hx_preterm"))
    hx_gdm = bool(form.get("hx_gdm"))
    fam_diabetes = bool(form.get("fam_diabetes"))
    hx_htn = bool(form.get("hx_htn"))
    # Normalized fields expected by risk modules
    pre_preg_diabetes = "Yes" if hx_gdm else "No"

    preg_interval_code = (
        preg_interval if preg_interval in ("4_11", "12_plus") else None
    )
    
    # Social / lifestyle
    smoking_status = (form.get("smoking_status") or "").strip()
    education_level = (form.get("education_level") or "").strip()
    insurance_type = (form.get("insurance_type") or "").strip()
    prenatal_start = _ival("prenatal_start")
    pregnancy_type = (form.get("pregnancy_type") or "").strip()
    zip_code = (form.get("zip") or "").strip()

    # Shared dict for the table lookup functions
    shared_inputs = {
        "age": age,
        "race": ethnicity,
        "bmi": bmi,
        "weight_pre": weight_pre,
        "height_feet": height_feet,
        "height_inches": height_inches,
        "prior_births": prior_births,
        "weeks_pregnant": weeks_pregnant,
        "preg_interval_code": preg_interval_code,
        "history_ptb": "Yes" if hx_preterm else "No",
        "prior_gdm": "Yes" if hx_gdm else "No",
        "pre_preg_diabetes": pre_preg_diabetes,
        "fam_history_diabetes": "Yes" if fam_diabetes else "No",
        "chronic_htn": "Yes" if hx_htn else "No",
        "smoking_status": smoking_status,
        "education_level": education_level,
        "insurance_type": insurance_type,
        "prenatal_start": prenatal_start,
        "pregnancy_type": pregnancy_type,
        "zip": zip_code,
        "PTB_XLSX_PATH": app.config.get("PTB_XLSX_PATH"),
        "GDM_XLSX_PATH": app.config.get("GDM_XLSX_PATH"),
        "GHT_XLSX_PATH": app.config.get("GHT_XLSX_PATH"),
    }

    # Maintain GDM’s historical input shape
    gdm_inputs = {
        "age": age,
        "race": ethnicity,
        "weight_pre": weight_pre,
        "height_feet": height_feet,
        "height_inches": height_inches,
        "gravida": _ival("gravida"),
        "prior_gdm": "Yes" if hx_gdm else "No",
        "fam_history_diabetes": "Yes" if fam_diabetes else "No",
        "chronic_htn": "Yes" if hx_htn else "No",
        "history_ptb": "Yes" if hx_preterm else "No",
        "pregnancy_type": pregnancy_type,
        "insurance_type": insurance_type,
        "prior_births": prior_births,
        "preg_interval": preg_interval,
    }

    results_payload = {"gdm": {}, "ptb": {}, "ght": {}}
    debug_blob = {}

    # ---- GDM ----
    try:
        gdm_res = gdm_lookup(gdm_inputs)
        debug_blob["gdm"] = gdm_res
        if gdm_res.get("ok"):
            results_payload["gdm"] = {
                "percent": gdm_res.get("risk_percent"),
                "bucket": gdm_res.get("bucket", "average"),
                "position": gdm_res.get("position", 50),
                "cohort_percent": gdm_res.get("cohort_percent"),
                "notes": gdm_res.get("notes", "")
            }
        else:
            flash(gdm_res.get("error") or "GDM lookup failed; defaulting to cohort average.")
    except Exception as e:
        flash(f"GDM risk calculation error: {e}")

    # ---- PTB ----
    try:
        ptb_res = ptb_lookup(shared_inputs)
        debug_blob["ptb"] = ptb_res
        if ptb_res.get("ok"):
            results_payload["ptb"] = {
                "percent": ptb_res.get("risk_percent"),
                "bucket": ptb_res.get("bucket", "average"),
                "position": ptb_res.get("position", 50),
                "cohort_percent": ptb_res.get("cohort_percent"),
                "notes": ptb_res.get("notes", "")
            }
        else:
            flash(ptb_res.get("error") or "PTB lookup failed; defaulting to cohort average.")
    except Exception as e:
        flash(f"PTB risk calculation error: {e}")

    # ---- GHT (Pregnancy-Related Hypertension) ----
    try:
        ght_res = ght_lookup(shared_inputs)
        debug_blob["ght"] = ght_res
        if ght_res.get("ok"):
            results_payload["ght"] = {
                "percent": ght_res.get("risk_percent"),
                "bucket": ght_res.get("bucket", "average"),
                "position": ght_res.get("position", 50),
                "cohort_percent": ght_res.get("cohort_percent"),
                "notes": ght_res.get("notes", "")
            }
        else:
            flash(ght_res.get("error") or "Hypertension lookup failed; defaulting to cohort average.")
    except Exception as e:
        flash(f"Hypertension risk calculation error: {e}")

    return render_template(
        "risk_result.html",
        results=results_payload,
        inputs=shared_inputs,
        debug=json.dumps(debug_blob, indent=2)
    )

# -------------------------------------------------------------
# Pregnancy Health News (Render parity with PythonAnywhere)
# -------------------------------------------------------------
@app.route("/news", methods=["GET"])
def news():
    max_total = 60
    max_per_category = 10

    # Preferred category order (any unknown categories will be appended after)
    preferred_order = [
        "Preterm Birth",
        "Hypertension of Pregnancy",
        "Gestational Diabetes",
        "Alcohol / Smoking / Substance Use",
        "Nutrition",
        "Exercise",
        "Mental Health / Support",
        "Other",
    ]

    try:
        resp = httpx.get(NEWS_FEED_URL, timeout=20.0)
        resp.raise_for_status()
        items = resp.json()
        if not isinstance(items, list):
            items = []
    except Exception as e:
        print("News feed fetch error:", e)
        flash("Pregnancy Health News is temporarily unavailable. Please try again later.")
        items = []

    by_category = {}
    total_added = 0

    # Fill up to 60 total, newest-first, with backfill across categories (cap 10 per category)
    for it in items:
        if total_added >= max_total:
            break

        if not isinstance(it, dict):
            continue

        cat = it.get("category") or "Other"
        if cat not in by_category:
            by_category[cat] = []

        if len(by_category[cat]) >= max_per_category:
            continue

        # Backward compatibility / fallbacks
        headline = it.get("headline") or it.get("plain_title") or it.get("pubmed_title") or "(No headline)"
        summary = it.get("summary") or it.get("plain_summary") or ""

        # Make a shallow copy so we don't mutate the original list
        it2 = dict(it)
        it2["headline"] = headline
        it2["summary"] = summary

        by_category[cat].append(it2)
        total_added += 1

    # Build category display order: preferred first, then any others
    seen = set()
    category_order = []

    for c in preferred_order:
        if c in by_category and by_category.get(c):
            category_order.append(c)
            seen.add(c)

    # Append any other categories present in the feed
    for c in by_category.keys():
        if c not in seen and by_category.get(c):
            category_order.append(c)

    resp_out = make_response(render_template("news.html", by_category=by_category, category_order=category_order))
    resp_out.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp_out.headers["Pragma"] = "no-cache"
    resp_out.headers["Expires"] = "0"
    return resp_out
    
@app.route("/references")
def references():
    static_dir = app.static_folder or "static"
    if not os.path.exists(os.path.join(static_dir, "babyq_full_evidence_protected.html")):
        flash("Evidence file not found in /static.")
        return redirect(url_for("index"))
    return send_from_directory(static_dir, "babyq_full_evidence_protected.html")

@app.route("/privacy")
def privacy():
    return render_template("privacy.html")

@app.route("/terms")
def terms():
    return render_template("terms.html")

@app.route("/feedback", methods=["GET", "POST"])
def feedback():
    if request.method == "POST":
        q1 = request.form.get("Question to improve", "")
        q2 = request.form.get("Feature suggestion", "")
        q3 = request.form.get("Additional comments", "")

        body = f"""
New feedback submitted from BabyQscore.org

Question to improve:
{q1}

Feature or improvement suggestion:
{q2}

Additional comments:
{q3}
"""

        try:
            import smtplib
            from email.message import EmailMessage

            msg = EmailMessage()
            msg["Subject"] = "New BabyQ Feedback"
            msg["From"] = "mgostine@babyqscore.org"
            msg["To"] = "mgostine@babyqscore.org"
            msg.set_content(body)

            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(os.environ.get("FEEDBACK_EMAIL_USER",""), os.environ.get("FEEDBACK_EMAIL_PASS",""))
                server.send_message(msg)

        except Exception as e:
            print("Feedback email error:", e)

        return redirect(url_for("feedback"))

    return render_template("feedback.html")

@app.route("/health")
def health():
    return {"ok": True, "app": "babyq_web", "time": datetime.datetime.utcnow().isoformat()}

@app.route("/sitemap.xml")
def sitemap():
    return send_from_directory(
        os.path.join(app.root_path, "static"),
        "sitemap.xml",
        mimetype="application/xml"
    )
@app.route("/")
def home_redirect():
    return redirect(url_for("landing"))

@app.route("/landing")
def landing():
    return render_template("landing.html")

@app.route("/stacy")
def stacy():
    return render_template("stacy.html")

@app.route("/chat", methods=["POST"])
def chat_proxy():
    try:
        data = request.get_json()
        message = data.get("message", "")
    except Exception:
        return jsonify({"response": "(Invalid request)"}), 400

    # Forward to Stacy API running on your home H100
    try:
        resp = httpx.post(
            os.environ.get("STACY_URL", "https://stacy-relay.onrender.com").rstrip("/") + "/chat",
            json={"message": message},
            timeout=60.0
        )
        reply = resp.json().get("response", "(No response from Stacy)")
    except Exception:
        reply = "(Error contacting Stacy API)"

    return jsonify({"response": reply})

if __name__ == "__main__":
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=debug)

# ---- END OF FILE babyq_web.py (line count -652) ----
