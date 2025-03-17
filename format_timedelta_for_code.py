def format_timedelta_for_code(td_obj):
    """Formats a timedelta object into a Python code string."""
    return f"timedelta(days={td_obj.days}, seconds={td_obj.seconds}, microseconds={td_obj.microseconds})"
