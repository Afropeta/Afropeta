
import psycopg2
import asyncio
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from datetime import datetime, timedelta

# Telegram bot token and Bot instance for sending notifications
TELEGRAM_BOT_TOKEN = '7812697260:AAFTOqIeuV2o4M93vSvu7e44SoOP9IeE8EA'
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# Database connection parameters
DB_PARAMS = {
    "dbname": "my_project_db",
    "user": "postgres",
    "password": "1201",
    "host": "localhost",
    "port": "5432"
}


async def notify_user(user_chat_id, job_summary, job_id):
    # Ensure the bot is properly initialized to get its username
    bot_info = await bot.get_me()
    apply_url = f"https://t.me/{bot_info.username}?start=apply_{job_id}"  # Construct the apply URL

    # InlineKeyboardButton for "Apply" with a URL
    apply_button = InlineKeyboardButton("Apply", url=apply_url)
    save_button = InlineKeyboardButton("Save", callback_data=f"save:{job_id}")
    reply_markup = InlineKeyboardMarkup([[apply_button, save_button]])

    # Send the message and capture the returned message object to get the message_id
    message = await bot.send_message(
        chat_id=user_chat_id,
        text=job_summary,
        parse_mode="Markdown",
        reply_markup=reply_markup
    )

    # Pin the sent message using the message_id
    await bot.pin_chat_message(
        chat_id=user_chat_id,
        message_id=message.message_id,
        disable_notification=True
    )


def format_job_summary(job):
    # Format salary/compensation summary
    if job[6]:  # salary_compensation
        salary_compensation = job[6]
        if float(salary_compensation).is_integer():
            salary_compensation = int(salary_compensation)
        salary_compensation_summary = f"{salary_compensation} {job[15]} {job[14]}"  # Include currency and compensation type
    else:
        salary_compensation_summary = f"{job[15]} {job[14]}"  # Show currency type and compensation type only

    # Gender icon logic
    applicant_gender = job[11]
    gender_icon = "⚥" if applicant_gender not in ["Male", "Female"] else "♂️" if applicant_gender == "Male" else "♀️"

    # Construct the job summary with the specified order
    job_summary = (
        f"🏷️ *Job Title:* {job[1]}\n\n"  # Job Title
        f"🕒 *Job Type:* {job[2]}, {job[8]}\n\n"  # Job Site and Job Type
        f"🏢 *Job Sector:* {job[3]}\n\n"  # Job Sector
        f"📍 *Work Location:* {job[5]}, {job[4]}\n\n"  # Location
        f"🎓 *Education Qualification:* {job[9]}\n\n"  # Education Qualification
        f"🎖️ *Experience Level:* {job[10]}\n\n"  # Experience Level
        f"💰 *Salary/Compensation:* {salary_compensation_summary}\n\n"  # Salary/Compensation
        f"👥 *Vacancy Number:* {job[13]}\n\n"  # Vacancy Number
        f"{gender_icon} *Applicant Gender:* {applicant_gender}\n\n"  # Applicant Gender with icon
        f"⏳ *Job Application Deadline:* {job[12]}\n\n"  # Application Deadline
        f"📄 *Job Description:*\n{job[7]}\n"  # Job Description
    )

    return job_summary


async def listen_for_new_jobs():
    conn = psycopg2.connect(**DB_PARAMS)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute("LISTEN new_job;")
    print("Listening for new job posts...")

    while True:
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            job_id = int(notify.payload)
            await handle_new_job(job_id)


async def handle_new_job(job_id):
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    # Retrieve the job details
    cursor.execute("""
        SELECT job_id, job_title, job_type, job_sector, location_country, location_city,
               salary_compensation, job_description, job_site, education_qualification,
               experience_level, applicant_gender, job_application_deadline, vacancy_number,
               compensation_type, currency_type
        FROM job_posts WHERE job_id = %s;
    """, (job_id,))

    job = cursor.fetchone()
    if not job:
        return

    cursor.execute("SELECT user_id, contact FROM users WHERE is_registered = 1;")
    users = cursor.fetchall()

    for user_id, user_chat_id in users:
        cursor.execute("""
            SELECT selected_sectors, experience_levels, job_sites, job_types, work_country, work_cities,
                   education_qualifications, salary_ranges, compensation_types, currency_type, 
                   vacancy_number, gender
            FROM user_job_alerts WHERE user_id = %s
        """, (user_id,))

        alert_data = cursor.fetchone()
        if not alert_data:
            continue

        user_alerts = {
            "selected_sectors": alert_data[0] or [],
            "experience_levels": alert_data[1] or [],
            "job_sites": alert_data[2] or [],
            "job_types": alert_data[3] or [],
            "work_country": alert_data[4],
            "work_cities": alert_data[5] or [],
            "education_qualifications": alert_data[6] or [],
            "salary_ranges": alert_data[7] or [],
            "compensation_types": alert_data[8] or [],
            "currency_type": alert_data[9],
            "vacancy_number": alert_data[10],
            "gender": alert_data[11] or []
        }

        if job_matches_criteria(job, user_alerts):
            job_summary = format_job_summary(job)
            await notify_user(user_chat_id, job_summary, job_id)
            track_notification(user_id, job_id)  # Mark as notified

    cursor.close()
    conn.close()

async def process_job_alerts():
    """Check for matching job alerts for each user and send notifications if matches are found."""
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    # Only select users with alert status turned ON
    cursor.execute(
        "SELECT user_id, contact FROM users WHERE is_registered = 1 AND user_id IN (SELECT user_id FROM user_job_alerts WHERE status = TRUE);")
    users = cursor.fetchall()

    for user_id, user_chat_id in users:
        cursor.execute("""
            SELECT selected_sectors, experience_levels, job_sites, job_types, work_country, work_cities,
                   education_qualifications, salary_ranges, compensation_types, currency_type, 
                   vacancy_number, gender
            FROM user_job_alerts WHERE user_id = %s
        """, (user_id,))

        alert_data = cursor.fetchone()
        if not alert_data:
            continue

        user_alerts = {
            "selected_sectors": alert_data[0] or [],
            "experience_levels": alert_data[1] or [],
            "job_sites": alert_data[2] or [],
            "job_types": alert_data[3] or [],
            "work_country": alert_data[4],
            "work_cities": alert_data[5] or [],
            "education_qualifications": alert_data[6] or [],
            "salary_ranges": alert_data[7] or [],
            "compensation_types": alert_data[8] or [],
            "currency_type": alert_data[9],
            "vacancy_number": alert_data[10],
            "gender": alert_data[11] or []
        }

        # Get jobs matching user criteria and send notifications
        matching_jobs = get_matching_jobs(user_alerts, user_id)

        for job in matching_jobs:
            job_summary = format_job_summary(job)
            await notify_user(user_chat_id, job_summary, job[0])
            track_notification(user_id, job[0])  # Record the sent notification

    cursor.close()
    conn.close()

def parse_salary_range(salary_range_str):
    """Parse a salary range string in the format '{min-max}'."""
    if not salary_range_str or '-' not in salary_range_str:
        return None, None
    try:
        min_salary, max_salary = map(int, salary_range_str.strip('{}').split('-'))
        return min_salary, max_salary
    except ValueError:
        return None, None



def get_matching_jobs(user_alerts, user_id):
    """Query the database to find jobs that match any of the user's alert criteria and haven't been notified yet."""
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    # Retrieve the last toggle-on timestamp
    cursor.execute("SELECT last_alert_toggle_on FROM user_job_alerts WHERE user_id = %s;", (user_id,))
    last_toggle_on = cursor.fetchone()[0]

    if not last_toggle_on:
        # If `last_alert_toggle_on` is not set, return an empty list since alerts are off or not initialized
        cursor.close()
        conn.close()
        return []

    # Set default values to None if any alert criteria are empty or missing
    job_types = user_alerts['job_types'] if user_alerts['job_types'] else None
    selected_sectors = user_alerts['selected_sectors'] if user_alerts['selected_sectors'] else None
    job_sites = user_alerts['job_sites'] if user_alerts['job_sites'] else None
    work_country = user_alerts['work_country'] if user_alerts['work_country'] else None
    work_cities = user_alerts['work_cities'] if user_alerts['work_cities'] else None
    education_qualifications = user_alerts['education_qualifications'] if user_alerts['education_qualifications'] else None
    experience_levels = user_alerts['experience_levels'] if user_alerts['experience_levels'] else None
    compensation_types = user_alerts['compensation_types'] if user_alerts['compensation_types'] else None
    currency_type = user_alerts['currency_type'] if user_alerts['currency_type'] else None
    vacancy_number = user_alerts['vacancy_number'] if user_alerts['vacancy_number'] else None
    gender = user_alerts['gender'] if user_alerts['gender'] else None

    # Parse salary range from user_alerts
    min_salary, max_salary = parse_salary_range(
        user_alerts['salary_ranges'][0] if user_alerts['salary_ranges'] else None)

    # Setting default values if parsing fails
    if min_salary is None:
        min_salary = 0  # Set to minimum if no lower bound is specified
    if max_salary is None:
        max_salary = float('inf')  # Set to maximum if no upper bound is specified

    # Set vacancy_number to None if it's empty or undefined
    vacancy_number = user_alerts['vacancy_number'] if user_alerts['vacancy_number'] else None

    query = """
        SELECT job_id,             -- 0
               job_title,          -- 1
               job_type,           -- 2
               job_sector,         -- 3
               location_country,   -- 4
               location_city,      -- 5
               salary_compensation,-- 6
               job_description,    -- 7
               job_site,           -- 8
               education_qualification, -- 9
               experience_level,   -- 10
               applicant_gender,   -- 11
               job_application_deadline, -- 12
               vacancy_number,     -- 13
               compensation_type,  -- 14
               currency_type,       -- 15
               created_at           --16
        FROM job_posts
        WHERE status_id = 5  -- Only include jobs with 'opened' status
        AND (
            job_type = ANY(%s) OR
            job_sector = ANY(%s) OR
            job_site = ANY(%s) OR
            location_country = %s OR
            location_city = ANY(%s) OR
            education_qualification = ANY(%s) OR
            experience_level = ANY(%s) OR
            compensation_type = ANY(%s) OR
            currency_type = %s OR
            (%s IS NULL OR vacancy_number = %s) OR  -- Adjusted to check for None instead of empty string
            applicant_gender = ANY(%s)
        )
        AND (%s IS NULL OR (salary_compensation BETWEEN %s AND %s))
        AND created_at >= %s  -- Only include jobs created after last_alert_toggle_on
        AND job_id NOT IN (
            SELECT job_id FROM job_notifications WHERE user_id = %s AND (notified = TRUE OR removed = TRUE)
        );
        """

    new_job_timeframe = datetime.now() - timedelta(hours=24)

    cursor.execute(query, (
        user_alerts['job_types'] or None,
        user_alerts['selected_sectors'] or None,
        user_alerts['job_sites'] or None,
        user_alerts['work_country'],
        user_alerts['work_cities'] or None,
        user_alerts['education_qualifications'] or None,
        user_alerts['experience_levels'] or None,
        user_alerts['compensation_types'] or None,
        user_alerts['currency_type'] or None,
        vacancy_number,  # Using the adjusted vacancy_number
        vacancy_number,
        user_alerts['gender'] or None,
        None if min_salary == 0 and max_salary == float('inf') else True,
        min_salary,
        max_salary,
        last_toggle_on,  # Only consider jobs created after last_alert_toggle_on
        user_id
    ))

    matching_jobs = cursor.fetchall()
    cursor.close()
    conn.close()

    return matching_jobs

def track_notification(user_id, job_id):
    """Mark job as notified and reset removed flag if it was removed."""
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO job_notifications (user_id, job_id, notified, removed)
        VALUES (%s, %s, TRUE, FALSE)
        ON CONFLICT (user_id, job_id) DO UPDATE SET notified = TRUE, removed = FALSE;
    """, (user_id, job_id))
    conn.commit()
    cursor.close()
    conn.close()

async def main_loop():
    """Main loop to run the job alert processing continuously every 5 minutes."""
    while True:
        await process_job_alerts()
        await asyncio.sleep(5)  # Delay of 5 minutes between checks

if __name__ == "__main__":
    asyncio.run(main_loop())