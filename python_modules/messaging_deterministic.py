from flask import session
from python_modules.messages_converted import MESSAGES

def _ensure_session_state():
    if 'loop_state' not in session or not isinstance(session['loop_state'], dict):
        session['loop_state'] = {}
        session.modified = True

def get_messages(question_num):
    """
    Return the next message (as a 1-element list) for a given 1-based question number.
    Loops through messages per question and persists position in Flask session.
    """
    # Normalize and ensure state
    q_str = str(int(question_num))
    _ensure_session_state()

    # Initialize pointer if new
    if q_str not in session['loop_state']:
        session['loop_state'][q_str] = 0
        session.modified = True

    # Pick the message list (fallback to default string)
    msg_list = MESSAGES.get(q_str, ["No messages available for this question."])

    # Get current message and advance pointer (looping)
    idx = session['loop_state'][q_str] % len(msg_list)
    message = msg_list[idx]
    session['loop_state'][q_str] = idx + 1
    session.modified = True  # important when mutating nested dicts

    return [message]

def get_next_message(question_num):
    """Used by /get_next_message; returns a single message string."""
    return get_messages(question_num)[0]
