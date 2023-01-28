import random
from flask import Flask, render_template

app = Flask(__name__)
base_url = "https://trello.com/b/XYqwyHd3/data-bi?filter=member:"
link_dict = {"Fellycia":"fellyciasutanto",
             "Sourav":"souravbhowmik8",
             "Mr. Jay":"jay_hakim",
             "Yeswanth":"yeswanth_s",
             "Bindu":"bindukodwaney2",
             "Vijay":"vpatel48",
             "Shailen":"shailenborkar",
             "James":"jconway27",
             "Ian":"ianbuckmaster",
             "Ann":"anntrachte",
             "Dan":"dfleury9",
             "Joy":"yliu94",
            "Glen":"glenkumo",
            "John":"johnlaciura",
             "Madhura":"madhurajoshi17",
             "All set Have a Nice Day :-) !!!":""}
names =  ["Fellycia","Sourav","Mr. Jay", "Yeswanth","Bindu", "Vijay", "Shailen","James","Ian","Ann","Dan","Joy","Glen","John", "Madhura"]

def index():
    while len(names) >= 1:
      name = (random.choice(names))
      names.remove(name)
      return name
    return "All set Have a Nice Day :-) !!!"
@app.route("/")
def home():
    global names
    names =  ["Fellycia","Sourav","Mr. Jay", "Yeswanth","Bindu", "Vijay", "Shailen","James","Ian","Ann","Dan","Joy","Glen","John", "Madhura"]
    return render_template("home.html")

@app.route("/next")
def next():
    name= index()
    if "!" in name:
        link = "/"
    else:
        link = base_url+link_dict[name]

    return render_template("next.html",next_name=name, link = link)

if __name__ == "__main__":
  app.run(debug=True)