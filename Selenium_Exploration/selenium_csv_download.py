from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# Specify the URL of ChatGPT
chatgpt_url = "https://filesamples.com/formats/csv#google_vignette"

# Create a new instance of Chrome WebDriver
driver = webdriver.Chrome()

try:

    driver.get(chatgpt_url)

    # Wait for the download button to be clickable (using a more specific locator)
    download_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.btn.btn-blue.col-span-3.md\\:col-span-1'))
    )

    driver.execute_script("arguments[0].scrollIntoView();", download_button)
    

    # Click the download button (assuming the button is clickable)
    download_button.click()

    # After clicking, handle any download dialogs if needed
    # For example, if it triggers a browser-based download dialog:
    WebDriverWait(driver, 10).until(EC.alert_is_present())
    driver.switch_to.alert.accept()  # Accept the download prompt

    # You can further handle the download process based on your specific needs
    # For instance, handling download paths, file types, etc.

finally:
    # Remember to quit the WebDriver after usage
    driver.quit()
