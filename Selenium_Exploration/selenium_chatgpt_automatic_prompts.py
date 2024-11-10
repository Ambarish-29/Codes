from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# Specify the URL of ChatGPT
chatgpt_url = "https://chat.openai.com/"

# Create a new instance of Chrome WebDriver
driver = webdriver.Chrome()

try:
    # Open ChatGPT in the web browser
    driver.get(chatgpt_url)

    # Wait for the chat input element to be visible
    chat_input = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.ID, "prompt-textarea"))
    )

    # Send a message to the chat input element
    chat_input.send_keys("hi, how are you? how is weather today?")
    chat_input.send_keys(Keys.RETURN)  # Press Enter to send the message

    time.sleep(10)

finally:
    # Close the browser
    driver.quit()
