# Property market analytics UI

Streamlit application utilising DuckDB for querying in-memory data (loaded from encrypted parquet files) alongside heavy caching due to the reactive architecture of Streamlit.

### Development

For local setup you should just `poetry install` the project.  

Consider setting up `pyenv` and `poetry` as per this [article](https://dorianbg.github.io/posts/python-project-setup-best-practice/).

After installing dependencies above, you could run locally with:  
```python -m streamlit run app.py``` . 

With PyCharm you can also utilise the debugger. 
Just make sure that inside your Run configuration module is set to `streamlit` and script parameters are set to `run app.py`.

### Data

The committed parquet files are encrypted, so you won't be able to re-use the full dataset locally. 
Instead us the `*_unencrypted.parquet` files to get a high level sense of the data.


### Screenshots 

General prices tab:
![Screenshot 2024-02-24 at 08.24.00.jpg](img%2FScreenshot%202024-02-24%20at%2008.24.00.jpg)

Location (municipality) specific tab:
![Screenshot 2024-02-24 at 08.24.15.jpg](img%2FScreenshot%202024-02-24%20at%2008.24.15.jpg)