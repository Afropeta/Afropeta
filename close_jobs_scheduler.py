import psycopg2
from datetime import datetime, timedelta
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Telegram bot setup
BOT_TOKEN = "7812697260:AAFTOqIeuV2o4M93vSvu7e44SoOP9IeE8EA"
bot = Bot(token=BOT_TOKEN)


async def notify_jobs_near_expiry():
    """Notify employers about job posts expiring in 24 hours (one-time notification)."""
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname="my_project_db",
            user="postgres",
            password="1201",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Calculate the 24-hour mark
        next_day = datetime.now() + timedelta(days=1)

        # Find job posts expiring in 24 hours and not yet notified
        cursor.execute('''
            SELECT job_id, job_title, job_application_deadline, user_id, channel_username, message_id
            FROM job_posts
            WHERE job_application_deadline BETWEEN %s AND %s
              AND status_id = (SELECT status_id FROM job_status WHERE status_name = 'opened')
              AND reminder_sent = FALSE
        ''', (datetime.now(), next_day))

        jobs_near_expiry = cursor.fetchall()

        for job in jobs_near_expiry:
            job_id, job_title, job_application_deadline, user_id, channel_username, message_id = job

            # Get employer contact and check expiry_alerts status
            cursor.execute('''
                SELECT u.contact, cp.expiry_alerts 
                FROM users u 
                JOIN company_profiles cp ON u.user_id = cp.user_id
                WHERE u.user_id = %s
            ''', (user_id,))
            employer_data = cursor.fetchone()

            if employer_data:
                employer_contact, expiry_alerts = employer_data

                # Create a job post link
                if channel_username and message_id:
                    job_link = f"https://t.me/{channel_username.lstrip('@')}/{message_id}"
                else:
                    job_link = "Job post link unavailable."

                # Format the expiration date
                formatted_deadline = job_application_deadline.strftime("%B %d")  # Example: "December 13"
                day_suffix = (
                    "st" if 11 > job_application_deadline.day % 10 == 1 else
                    "nd" if 11 > job_application_deadline.day % 10 == 2 else
                    "rd" if 11 > job_application_deadline.day % 10 == 3 else "th"
                )
                formatted_deadline += day_suffix + job_application_deadline.strftime(", %Y")  # Example: "December 13th, 2024"

                # Notify the employer via Telegram
                message = (
                    f"🔔 <b>Reminder</b>\n\n"
                    f"Your job post '<a href='{job_link}'>{job_title}</a>' is expiring soon!\n\n"
                    f"Expiration Date: <b>{formatted_deadline}</b>\n\n"
                    f"Please consider reposting or extending the job post if you wish to keep it active."
                )
                await bot.send_message(
                    chat_id=employer_contact,
                    text=message,
                    parse_mode="HTML",
                    disable_notification=not expiry_alerts  # Send silently if expiry_alerts is FALSE
                )

                # Mark the job as notified
                cursor.execute('''
                    UPDATE job_posts
                    SET reminder_sent = TRUE
                    WHERE job_id = %s
                ''', (job_id,))

        conn.commit()
        logger.info(f"Notified {len(jobs_near_expiry)} employers about expiring jobs.")

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error while notifying jobs near expiry: {error}")
    finally:
        if conn:
            cursor.close()
            conn.close()


async def close_expired_jobs():
    """Close jobs that have passed their deadline, notify employers, remove 'Apply' buttons, and add 'Job Closed' to the summary."""
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname="my_project_db",
            user="postgres",
            password="1201",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Fetch the status_id for 'closed'
        cursor.execute('''
            SELECT status_id FROM job_status WHERE status_name = 'closed'
        ''')
        closed_status_id = cursor.fetchone()[0]

        # Find jobs that are past their deadline and not yet closed
        cursor.execute('''
            SELECT job_id, channel_username, message_id, job_title, job_site, job_type, job_sector,
                   location_city, location_country, education_qualification, experience_level,
                   salary_compensation, currency_type, compensation_type, vacancy_number,
                   applicant_gender, job_application_deadline, job_description, user_id
            FROM job_posts
            WHERE job_application_deadline < %s
              AND status_id != %s
        ''', (datetime.now(), closed_status_id))

        expired_jobs = cursor.fetchall()

        for job in expired_jobs:
            (job_id, channel_username, message_id, job_title, job_site, job_type, job_sector,
             location_city, location_country, education_qualification, experience_level,
             salary_compensation, currency_type, compensation_type, vacancy_number,
             applicant_gender, job_application_deadline, job_description, user_id) = job

            # Format the application deadline
            if job_application_deadline:
                formatted_deadline = job_application_deadline.strftime("%B %d")
                day_suffix = (
                    "st" if job_application_deadline.day % 10 == 1 and job_application_deadline.day != 11 else
                    "nd" if job_application_deadline.day % 10 == 2 and job_application_deadline.day != 12 else
                    "rd" if job_application_deadline.day % 10 == 3 and job_application_deadline.day != 13 else "th"
                )
                formatted_deadline += day_suffix + job_application_deadline.strftime(", %Y")
            else:
                formatted_deadline = "No deadline"

            # Update the job status to 'closed'
            cursor.execute('''
                UPDATE job_posts
                SET status_id = %s
                WHERE job_id = %s
            ''', (closed_status_id, job_id))

            # Construct "Job Closed" message
            salary_compensation_summary = (
                f"{int(salary_compensation) if float(salary_compensation).is_integer() else salary_compensation} "
                f"{currency_type} {compensation_type}" if salary_compensation else f"{currency_type} {compensation_type}"
            )
            gender_icon = {"Male": "♂️", "Female": "♀️"}.get(applicant_gender, "⚥")

            job_summary = (
                f"🏷️ *Job Title:* {job_title}\n\n"
                f"🕒 *Job Type:* {job_site}, {job_type}\n\n"
                f"🏢 *Job Sector:* {job_sector}\n\n"
                f"📍 *Work Location:* {location_city}, {location_country}\n\n"
                f"🎓 *Education Qualification:* {education_qualification}\n\n"
                f"🎖️ *Experience Level:* {experience_level}\n\n"
                f"💰 *Salary/Compensation:* {salary_compensation_summary}\n\n"
                f"👥 *Vacancy Number:* {vacancy_number}\n\n"
                f"{gender_icon} *Applicant Gender:* {applicant_gender}\n\n"
                f"⏳ *Job Application Deadline:* {formatted_deadline}\n\n"
                f"📄 *Job Description:*\n{job_description}\n"
                f"{'\\_' * 30}\n\n"
                f"🚫 *Job Closed*\n"
            )

            # Edit the Telegram post
            if channel_username and message_id:
                try:
                    logger.info(f"Editing job post ID {job_id} for channel @{channel_username.lstrip('@')} with message ID {message_id}")
                    await bot.edit_message_text(
                        chat_id=f"@{channel_username.lstrip('@')}",
                        message_id=message_id,
                        text=job_summary,
                        parse_mode="Markdown"
                    )
                    await bot.edit_message_reply_markup(
                        chat_id=f"@{channel_username.lstrip('@')}",
                        message_id=message_id,
                        reply_markup=None  # Removes the inline buttons
                    )
                    logger.info(f"Successfully updated job post ID {job_id} with 'Job Closed'.")
                except Exception as e:
                    logger.error(f"Failed to update job post ID {job_id}: {e}")

            # Notify the employer
            cursor.execute('''
                SELECT contact FROM users WHERE user_id = %s
            ''', (user_id,))
            employer_contact = cursor.fetchone()

            if employer_contact:
                employer_contact = employer_contact[0]
                job_link = f"https://t.me/{channel_username.lstrip('@')}/{message_id}" if channel_username and message_id else "Job post link unavailable."

                notification_message = (
                    f"📢 <b>Job Closed Notification</b>\n\n"
                    f"Your job post '<a href='{job_link}'>{job_title}</a>' has been closed as it has passed its application deadline ({formatted_deadline}).\n\n"
                    f"Thank you for using our service. You may repost the job if needed."
                )

                try:
                    await bot.send_message(chat_id=employer_contact, text=notification_message, parse_mode="HTML")
                    logger.info(f"Notified employer (user_id: {user_id}) about closed job ID {job_id}.")
                except Exception as e:
                    logger.error(f"Failed to notify employer (user_id: {user_id}) for job ID {job_id}: {e}")

        conn.commit()
        logger.info(f"{len(expired_jobs)} jobs were closed and updated successfully.")

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error while closing expired jobs: {error}")
    finally:
        if conn:
            cursor.close()
            conn.close()


def start_scheduler():
    """Start the job scheduler for closing expired jobs and notifying near-expiry jobs."""
    scheduler = AsyncIOScheduler()

    # Add asynchronous jobs to the scheduler
    scheduler.add_job(close_expired_jobs, 'interval', minutes=1)
    scheduler.add_job(notify_jobs_near_expiry, 'interval', minutes=1)

    scheduler.start()
    logger.info("Scheduler started. Closing expired jobs and notifying near-expiry jobs every minute.")


if __name__ == "__main__":
    import asyncio

    # Start the scheduler
    start_scheduler()

    # Keep the script running
    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler.")
