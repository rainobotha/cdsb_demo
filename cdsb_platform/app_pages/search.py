import streamlit as st
import json

import os

conn = st.connection("snowflake")
session = conn.session()
DB = st.session_state.get("cdsb_database", "CDSB_DEMO")
SCHEMA = st.session_state.get("cdsb_schema", "RAW")

SEARCH_SERVICE = os.getenv("CDSB_SEARCH_SERVICE", f"{DB}.{SCHEMA}.QLD_GOVERNMENT")
AGENT_NAME = os.getenv("CDSB_AGENT_NAME", f"{DB}.{SCHEMA}.CDSB_ASSISTANT")


def search_cortex(query, limit=10):
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.SEARCH(
            '{SEARCH_SERVICE}',
            '{query.replace("'", "''")}',
            {{
                'columns': ['chunk_content', 'title', 'source_url', 'domain'],
                'limit': {limit}
            }}
        ) as results
    """).collect()
    if result:
        return json.loads(result[0][0])
    return {"results": []}


tab_search, tab_chat = st.tabs(["Search", "Chat with Agent"])

with tab_search:
    st.markdown("Search across all crawled documents using Cortex Search.")

    query = st.text_input("Search query", placeholder="e.g. road safety regulations Queensland")

    sc1, sc2 = st.columns([1, 4])
    with sc1:
        limit = st.number_input("Results", min_value=1, max_value=50, value=10)

    if query:
        with st.spinner("Searching..."):
            results = search_cortex(query, limit=limit)

        hits = results.get("results", [])
        st.markdown(f"**{len(hits)} results**")

        for i, hit in enumerate(hits):
            with st.expander(
                f"{i+1}. {hit.get('title', 'Untitled')} — {hit.get('domain', '')}",
                expanded=(i < 3),
            ):
                st.caption(hit.get("source_url", ""))
                st.markdown(hit.get("chunk_content", "")[:2000])

with tab_chat:
    st.markdown("Chat with the CDSB Assistant powered by Cortex Agent.")

    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []

    for msg in st.session_state.chat_messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    if prompt := st.chat_input("Ask about Queensland government services..."):
        st.session_state.chat_messages.append({"role": "user", "content": prompt})

        with st.chat_message("user"):
            st.write(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    search_results = search_cortex(prompt, limit=5)
                    context_parts = []
                    for hit in search_results.get("results", []):
                        context_parts.append(
                            f"Source: {hit.get('title', '')} ({hit.get('domain', '')})\n"
                            f"{hit.get('chunk_content', '')[:1500]}"
                        )
                    context = "\n\n---\n\n".join(context_parts)

                    system_prompt = (
                        "You are the CDSB Queensland Assistant. Answer questions using ONLY the context provided. "
                        "If the context doesn't contain relevant information, say so. "
                        "Always cite your sources with the document title and domain."
                    )

                    full_prompt = f"{system_prompt}\n\nContext:\n{context}\n\nQuestion: {prompt}"

                    result = session.sql(
                        "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS resp",
                        params=["claude-3-5-sonnet", full_prompt],
                    ).collect()
                    response = result[0][0] if result else "No response."
                    st.write(response)
                except Exception as e:
                    response = f"Error: {e}"
                    st.error(response)

        st.session_state.chat_messages.append({"role": "assistant", "content": response})
