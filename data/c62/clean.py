import pandas as pd

df = pd.read_csv('c62.csv')

chapters = {
    'hscodes' : [],
    'descriptions' : [],
}

headings = {
    'hscodes' : [],
    'descriptions' : [],
}

subheadings = {
    'hscodes' : [],
    'descriptions' : [],
}

country_wise = {
    'hscodes' : [],
    'descriptions' : [],
}



for row in df.itertuples():

    code = str(row[1])
    description = row[2]

    if len(code) == 2:
        chapters['hscodes'].append(code)
        chapters['descriptions'].append(description)
    elif len(code) == 4:
        headings['hscodes'].append(code)
        headings['descriptions'].append(description)
    elif len(code) == 6:
        subheadings['hscodes'].append(code)
        subheadings['descriptions'].append(description)
    elif len(code) == 10 and code[-4:] == '0000':
        subheadings['hscodes'].append(code[:-4])
        subheadings['descriptions'].append(description)
    else:
        country_wise['hscodes'].append(code)
        country_wise['descriptions'].append(description)




df = pd.DataFrame(chapters)
df.to_csv('chapters.csv', index=False)

df = pd.DataFrame(headings)
df.to_csv('headings.csv', index=False)

df = pd.DataFrame(subheadings)
df.to_csv('subheadings.csv', index=False)

df = pd.DataFrame(country_wise)
df.to_csv('country_wise.csv', index=False)