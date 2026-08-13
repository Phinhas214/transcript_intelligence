from airflow.sdk import dag, task, Variable
from pendulum import datetime
from airflow.providers.standard.sensors.filesystem import FileSensor
from google import genai
import yt_dlp
import json
from pathlib import Path
from youtube_transcript_api import YouTubeTranscriptApi
from airflow.providers.smtp.hooks.smtp import SmtpHook
import html
import markdown
from datetime import timedelta

INCLUDE_DIR = Path(__file__).resolve().parent.parent / "include"

default_args = {
    'retries': 2, 
    'retry_delay': timedelta(minutes=5), 
    'retry_exponential_backoff': True, 

}

@dag(
    tags=["summary"],
    description="DAG to extract info out of youtube transcripts",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    default_args=default_args,
)


def summarize_transcript():

    @task
    def extract_metadata():
        channel_url = "https://www.youtube.com/@ApacheAirflow/videos"
        ydl_opts = {"extract_flat": True, "quiet": True}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(channel_url, download=False)
        entries = info.get("entries") or []

        return [{"id": e["id"], "title": e["title"]} for e in entries]

    @task
    def load_metadata_to_file(metadata):
        with open(INCLUDE_DIR / 'apache_airflow_yt_metadata.json', 'w', encoding='utf-8') as json_file:
            json.dump(metadata, json_file, indent=2, ensure_ascii=False)

    @task
    def diff_new_videos(metadata):
        seen_ids = Variable.get("yt_seen_video_ids", default=None, deserialize_json=True)

        if seen_ids is None:
            # first run ever: treat everything currently on the channel as
            # already seen so only future uploads trigger a digest
            Variable.set(
                "yt_seen_video_ids",
                [v["id"] for v in metadata],
                serialize_json=True,
            )
            return []

        seen_ids = set(seen_ids)
        return [v for v in metadata if v["id"] not in seen_ids]

    @task
    def extract_transcripts(video: dict):
        ytt_api = YouTubeTranscriptApi()
        fetched_transcript = ytt_api.fetch(video["id"])
        transcript_text = " ".join(snippet.text for snippet in fetched_transcript)

        return {
            "id": video["id"],
            "title": video["title"],
            "transcript": transcript_text,
        }


    @task
    def summarize(video: dict):
        client = genai.Client()

        prompt = f"""
        Analyze this video transcript and return (only return what's asked nothing more):
        1. A short summary
        2. Any action items
        3. Tools, technologies, or companies mentioned
        4. Important decisions or opinions

        Transcript:
        {video["transcript"]}
        """

        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
        )

        return {
            "id": video["id"],
            "title": video["title"],
            "summary": response.text,
        }

    @task.short_circuit
    def has_new_videos(summaries: list[dict]):
        return len(summaries) > 0

    @task
    def combine_digest(summaries: list[dict]):

        sections = []
        for video in summaries:
            url = f"https://www.youtube.com/watch?v={video['id']}"
            title = html.escape(video["title"])
            summary = markdown.markdown(video["summary"])
            sections.append(f'<h2><a href="{url}">{title}</a></h2><p>{summary}</p>')

        return "<h1>Apache Airflow Newsletter</h1>" + "".join(sections)

    @task
    def send_digest_email(digest_html: str):

        recipient = Variable.get("digest_recipient_email")
        with SmtpHook() as hook:
            hook.send_email_smtp(
                to=recipient,
                subject="Apache Airflow YouTube Digest",
                html_content=digest_html,
            )

    @task
    def mark_as_seen(new_videos: list[dict]):
        seen_ids = set(Variable.get("yt_seen_video_ids", default=[], deserialize_json=True))
        seen_ids.update(v["id"] for v in new_videos)
        Variable.set("yt_seen_video_ids", list(seen_ids), serialize_json=True)

    # combine_summaries
    # extract_action_items
    # save_report
    metadata = extract_metadata()
    load_metadata_to_file(metadata)
    new_videos = diff_new_videos(metadata)
    transcripts = extract_transcripts.expand(video=new_videos)
    summaries = summarize.expand(video=transcripts)

    should_send = has_new_videos(summaries)
    digest = combine_digest(summaries)
    email_sent = send_digest_email(digest)
    seen_marked = mark_as_seen(new_videos)

    should_send >> digest
    email_sent >> seen_marked

summarize_transcript()

