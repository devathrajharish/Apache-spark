import streamlit as st

st.title("Hello whatsapp?")
st.sidebar.file_uploader("upload your files", type=['jpeg', 'jpg'])
st.set_option('deprecation.showfileUploaderEncoding', False)
