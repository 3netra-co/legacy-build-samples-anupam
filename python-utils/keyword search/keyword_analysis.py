#The code was written as part of an AWS Glue Job
#Please replace OpenAI, Cluade, Gemini, Google Token Credentials in the code with the dummies

from typing import List

import streamlit as st
import requests
from bs4 import BeautifulSoup
import openai
import google.generativeai as genai
import anthropic

import boto3
from botocore.exceptions import ClientError

import json
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

results_file_name = r"results.json"
with open(results_file_name) as file:
    response_d = json.load(file)


def get_secret():
    secret_name = "SECRET"
    region_name = "REGION"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    return secret


def get_text_and_title(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        found_title = soup.find('title').text

        tags_of_interest = ['div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'span', 'p']
        elements = soup.find_all(tags_of_interest)
        found_page_text = " ".join(element.get_text(separator=" ", strip=True) for element in elements)

        # print("titlw",found_title)
        # print("text",found_page_text)

        return found_page_text, found_title
    else:
        print("Failed to retrieve the webpage")
        return "", ""


def get_chatgpt_keyword(url: str, title: str, user_text: str):
    # return response_d[url + "_chatgpt"]
    response_key = url + "_chatgpt"
    if response_key in response_d:
        return response_d[response_key]

    else:
        openai.api_key = 'sk-'
        prompt = f"""{user_text} \n as a professional google ads professional, I want to create google ads for {title}. Help me to find most effective phrase match or exact match type keywords along with negative keyword to laser target my audience and drive optimum conversions.
                                   write at least 30 positive keywords which can I target from text
                                   output like json
                                   {{
                                       "keywords": [list of keywords"],
                                       "negative keywords": [list of --kw keywords"]
                                   }}"""
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",  # Specify the "gpt-3.5-turbo" model
            messages=[{"role": "system", "content": prompt}],
            # max_tokens=21391
        )

        # data = json.loads(response.json())
        data = response
        res_text = json.loads(data['choices'][0]['message']['content'])

        response_d[response_key] = res_text

        with open(results_file_name, 'w') as file:
            json.dump(response_d, fp=file)

        return res_text


def get_gemini_keyword(url: str, title: str, user_text: str):
    response_key = url + "_gemini"
    if response_key in response_d:
        return response_d[response_key]
    else:
        genai.configure(api_key="AI")
        model = genai.GenerativeModel()
        prompt = f"""{user_text} \n as a professional google ads professional, I want to create google ads for {title}. Help me to find most effective phrase match or exact match type keywords along with negative keyword to laser target my audience and drive optimum conversions.
                write at least 30 positive keywords which can I target from text
                output like json
                {{
                    "keywords": [list of keywords"],
                    "negative keywords": [list of --kw keywords"]
                }}"""
        response = model.generate_content(prompt)
        res_text = get_keyword_dictionary_from_claude_api_response(response.text)
        response_d[response_key] = res_text

        with open(results_file_name, 'w') as file:
            json.dump(response_d, fp=file)

        return res_text


def get_cluade_keyword(url: str, title: str, user_text: str):
    response_key = url + "_cluade"
    if response_key in response_d:
        return response_d[response_key]

    else:

        client = anthropic.Anthropic(
            # defaults to os.environ.get("ANTHROPIC_API_KEY")
            api_key="sk-",
        )

        prompt = f"""{user_text} \n as a professional google ads professional, I want to create google ads for {title}. Help me to find most effective phrase match or exact match type keywords along with negative keyword to laser target my audience and drive optimum conversions.
               write at least 30 positive keywords which can I target from text
               output like json
               {{
                   "keywords": [list of keywords"],
                   "negative keywords": [list of --kw keywords"]
               }}"""

        response = client.messages.create(
            model="claude-2.0",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}]
            # prompt,
            # max_tokens_to_sample=150,  # Maximum number of tokens to generate
            #
        )
        res_text = response.content[0].text
        response_d[response_key] = res_text

        with open(results_file_name, 'w') as file:
            json.dump(response_d, fp=file)

        return get_keyword_dictionary_from_claude_api_response(res_text)


def get_keyword_dictionary_from_claude_api_response(api_response: str) -> dict:
    text_found = api_response[api_response.find("{"):api_response.rfind("}", 1) + 1]
    keyword_d = json.loads(text_found)
    return keyword_d


def normalise_keyword(keyword_s: str, all_keywords) -> str:
    # words = re.findall(r'\b\w+\b', sentence.lower())
    # word_counts = {}
    # for word in words:
    #     word_counts[word] = word_counts.get(word, 0) +
    keyword = "".join(ch if ch.isalnum() else ' ' for ch in keyword_s.lower()).replace("  ", ' ').strip().title()

    if keyword not in all_keywords:
        all_keywords.add(keyword)
        return keyword
    return None


def findKeyword(url, title, text):
    chatgpt_keyword = get_chatgpt_keyword(url, title, text)
    gemini_keyword = get_gemini_keyword(url, title, text)
    cluade_keyword = get_cluade_keyword(url, title, text)

    # Merge keywords
    keywords = []
    all_keywords = set()
    keywords.extend((normalise_keyword(keyword, all_keywords), 'chatgpt') for keyword in chatgpt_keyword["keywords"])
    keywords.extend((normalise_keyword(keyword, all_keywords), 'gemini') for keyword in gemini_keyword["keywords"])
    keywords.extend((normalise_keyword(keyword, all_keywords), 'claude') for keyword in cluade_keyword["keywords"])
    st.write(len(keywords))
    merged_keywords = sorted((k for k in keywords if isinstance(k[0], str)), key=lambda i: len(i[0]))

    all_negative_keywords = []
    all_keywords = set()
    all_negative_keywords.extend(
        (normalise_keyword(keyword, all_keywords), 'chatgpt') for keyword in chatgpt_keyword["negative keywords"])
    all_negative_keywords.extend(
        (normalise_keyword(keyword, all_keywords), 'gemini') for keyword in gemini_keyword["negative keywords"])
    all_negative_keywords.extend(
        (normalise_keyword(keyword, all_keywords), 'claude') for keyword in cluade_keyword["negative keywords"])

    # Merge negative keywords
    merged_negative_keywords = sorted((k for k in all_negative_keywords if isinstance(k[0], str)), key=lambda i: i[0])

    # Create the final dictionary
    merged_dict = {
        "keywords": merged_keywords,
        "negative keywords": merged_negative_keywords
    }
    return merged_dict


def keyword_planner(keywords: [], customer_id):
    secret_value = json.loads(get_secret())
    GoogleCreds = {
        "client_id": secret_value["CLIENT_ID"],
        "client_secret": secret_value["CLIENT_SECRET"],
        "developer_token": secret_value["DEVELOPER_TOKEN"],
        "login_customer_id": secret_value["LOGIN_CUSTOMER_ID"],
        "refresh_token": secret_value["REFRESH_TOKEN"],
        "use_proto_plus": secret_value["USE_PROTO_PLUS"]
    }

    client = GoogleAdsClient.load_from_dict(GoogleCreds, "v14")
    # keyword planner

    keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")
    request = client.get_type("GenerateKeywordIdeasRequest")
    request.customer_id = customer_id

    request.language = "languageConstants/1000"  # English
    request.geo_target_constants.append("geoTargetConstants/2840")  # United States
    request.include_adult_keywords = False
    request.keyword_plan_network = client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH_AND_PARTNERS

    keyword_seed = client.get_type("KeywordSeed")

    for keyword_text in keywords:
        keyword_seed.keywords.append(keyword_text)
    request.keyword_seed = keyword_seed

    response = keyword_plan_idea_service.generate_keyword_ideas(request=request)

    results = []

    for idea in response.results:
        metrics = idea.keyword_idea_metrics
        # print(metrics)
        result = {
            "keyword": idea.text,
            "average_search_volume": metrics.avg_monthly_searches,
            "competition": metrics.competition,

        }
        print(
            f"Keyword: {idea.text}, Search Volume: {metrics.avg_monthly_searches}, Competition: {metrics.competition}, Competition Index: {metrics.competition_index}")


if __name__ == '__main__':
    st.title('Google ADS Keyword Analysis')
    st.subheader("Step 1: Enter Your web URL")
    st_url = st.text_input("Enter Website URL: ", label_visibility="hidden", placeholder="Enter Website URL: ")
    # found_title, found_page_text = "", ""

    if st_url:
        found_page_text, found_title = get_text_and_title(st_url)
        # st.write(f'Hello Your URL is {url}')
        # st.write(f"text is {page_text}")
        st.subheader("Step 2: Add Title and Page Text")
        title = st.text_input("Found Title", found_title, placeholder='Enter Page Title')
        page_text = st.text_area("Found Page Text", found_page_text, height=250, placeholder='Enter Page content')

        if st.button("Find Keyword", type="primary", use_container_width=True):
            with st.spinner('Loading....'):
                keywords_d = findKeyword(st_url, found_title, found_page_text)
                st.write(keywords_d)
