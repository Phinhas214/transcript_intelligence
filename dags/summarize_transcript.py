from airflow.sdk import dag, task, Variable
from pendulum import datetime
from airflow.providers.standard.sensors.filesystem import FileSensor
from google import genai
import yt_dlp
import json
from youtube_transcript_api import YouTubeTranscriptApi

@dag(
    tags=["summary"],
    description="DAG to extract info out of youtube transcripts",
    schedule="@daily",
    start_date=datetime(2026, 1, 1)
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
        with open('../include/apache_airflow_yt_metadata.json', 'w', encoding='utf-8') as json_file:
            json.dump(metadata, json_file, indent=2, ensure_ascii=False)

    @task
    def diff_new_videos(metadata):
        seen_ids = Variable.get("yt_seen_video_ids", default_var=None, deserialize_json=True)

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

    # extract a transcript per new video
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
        Analyze this video transcript and return:
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


    # combine_summaries
    # extract_action_items
    # save_report
    metadata = extract_metadata()
    load_metadata_to_file(metadata)
    new_videos = diff_new_videos(metadata)
    transcripts = extract_transcripts.expand(video=new_videos)
    summaries = summarize.expand(video=transcripts)

summarize_transcript()

