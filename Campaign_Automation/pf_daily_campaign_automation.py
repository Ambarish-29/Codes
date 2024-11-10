import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

links_list = ['links.csv','links_1.csv','links_2.csv', 'links_3.csv']

pf_df = pd.read_csv('pf.csv')
len_pf_df = len(pf_df)
i = 0
links_count = 0


class MyCustomError(Exception):
    pass


for link_file in links_list:
    links_df = pd.read_csv(link_file)
    links_null_df = links_df[links_df['rim_no'].isnull()].drop(columns='rim_no').reset_index(drop=True)
    links_count += len(links_null_df)


try:
    if links_count < len_pf_df :
        #Raise custom error if condition is satisfied
        raise MyCustomError("Not enough links files to proceed with the activity. Kindly provide more additional files to continue")
    

    for link_file in links_list:
        links_df = pd.read_csv(link_file)
        links_null_df = links_df[links_df['rim_no'].isnull()].drop(columns='rim_no').reset_index(drop=True)
        if len(links_null_df) == 0:
            continue
        links_not_null_df = links_df[links_df['rim_no'].notnull()]
        links_used_df = links_null_df.head(len_pf_df)['link'].reset_index(drop=True)
        pf_df_frst = pf_df.head(len(links_used_df)).reset_index(drop=True)
        pf_df_scnd = pf_df.tail(len_pf_df - len(links_used_df)).reset_index(drop=True)
        df_combined_tmp = pd.concat([pf_df_frst,links_used_df], axis=1)
        pf_df = pf_df_scnd
        rim_no_df = df_combined_tmp['rim_no']
        link_null_rim_combined = pd.concat([links_null_df,rim_no_df], axis=1)
        union_df = pd.concat([links_not_null_df, link_null_rim_combined], axis=0).reset_index(drop=True)
        union_df.to_csv(link_file, index=False)
        df_combined = pd.concat([df_combined,df_combined_tmp], axis=0) if i != 0 else df_combined_tmp
        i+=1
        if len(links_null_df) > len_pf_df:
            break
        else:
            len_pf_df = len_pf_df - len(links_null_df)

    df_combined = df_combined.reset_index(drop=True)
    df_combined.to_csv('pf.csv', index=False)


except MyCustomError as e:
    #Define email parameters
    sender_email = "ambarish2908@gmail.com"
    receiver_email = "asrinivasan064@gmail.com"
    password = "fyrg rtia fjco sxmd"
    subject = "Error Notification"

    #Create email message
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    body = f"An error occurred: {str(e)}"

    message.attach(MIMEText(body, "plain"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())

    raise e
