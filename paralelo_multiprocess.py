import multiprocessing
from multiprocessing.context import Process
import pandas as pd  
import numpy as np  
import matplotlib.pyplot as plt  
import time
import os
import nltk
from nltk.corpus import stopwords
from collections import Counter

names = ['score', 'title', 'review']
df = pd.read_csv('amazon_review_full_csv/train.csv', names=names)  

df_1 = df.loc[df['score'] == 1]
df_2 = df.loc[df['score'] == 2]
df_3 = df.loc[df['score'] == 3]
df_4 = df.loc[df['score'] == 4]
df_5 = df.loc[df['score'] == 5]

data_amount = [int(df_1.size), int(df_1.size)*2, int(df_1.size)*3, int(df_1.size)*4, int(df_1.size)*5]
tiempoProcesos = []
tiempoSecuencial = []  

#nltk.download('stopwords')
stop_words = stopwords.words('english')

class ProcessCount(multiprocessing.Process):

    def __init__(self, name, dfCount):
      multiprocessing.Process.__init__(self)
      self.name = name
      self.dfCount = dfCount

    def run(self):            
        print ("---> " + self.name + " corriendo, con ID de proceso " + str(os.getpid()) + "\n")
        # PONER TODO EL TEXTO EN MINUSCULAS
        self.dfCount.loc[:,'review'] = self.dfCount.loc[:,'review'].str.lower()
        # REMOVER SIGNOS DE PUNTUACION
        self.dfCount.loc[:,'review'] = self.dfCount.loc[:,'review'].str.replace('[^\w\s]','')
        self.dfCount.loc[:,'review'] = self.dfCount.loc[:,'review'].str.replace('would','')
        # ELIMINAR LAS STOP WORDS
        self.dfCount.loc[:,'review'] = self.dfCount.loc[:,'review'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop_words)]))
        
        # CONTAR LAS PALABRAS DE LAS REVIEWS
        results = Counter(" ".join(self.dfCount['review']).split())
        print(f"PARA LA PUNTUACION {self.dfCount.iloc[0]['score']}: {results.most_common(20)}")

def count_words(dfCount):
    dfCount.loc[:,'review'] = dfCount.loc[:,'review'].str.lower()
    dfCount.loc[:,'review'] = dfCount.loc[:,'review'].str.replace('[^\w\s]','')
    dfCount.loc[:,'review'] = dfCount.loc[:,'review'].str.replace('would','')
    dfCount.loc[:,'review'] = dfCount.loc[:,'review'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop_words)]))
    results = Counter(" ".join(dfCount['review']).split())
    print(f"PARA LA PUNTUACION {dfCount.iloc[0]['score']}")
        
def main():
    
    process1 = ProcessCount("Process#1 ", df_1)
    process2 = ProcessCount("Process#2 ", df_2)
    process3 = ProcessCount("Process#3 ", df_3)
    process4 = ProcessCount("Process#4 ", df_4)
    process5 = ProcessCount("Process#5 ", df_5)
    
    inicio = time.time()
    process1.start()
    process2.start()
    process3.start()
    process4.start()
    process5.start()

    process1.join()
    tiempoProcesos.append(time.time()-inicio)
    process2.join()
    tiempoProcesos.append(time.time()-inicio)
    process3.join()
    tiempoProcesos.append(time.time()-inicio)
    process4.join()
    tiempoProcesos.append(time.time()-inicio)
    process5.join()
    tiempoProcesos.append(time.time()-inicio)

    inicio = time.time() 
    count_words(df_1)
    tiempoSecuencial.append(time.time()-inicio)
    count_words(df_2)
    tiempoSecuencial.append(time.time()-inicio)
    count_words(df_3)
    tiempoSecuencial.append(time.time()-inicio)
    count_words(df_4)
    tiempoSecuencial.append(time.time()-inicio)
    count_words(df_5)
    tiempoSecuencial.append(time.time()-inicio)

    plt.figure(figsize=(10, 5))
    plt.ticklabel_format(axis='x', style='plain')
    plt.plot(data_amount, tiempoSecuencial, "o-")
    plt.plot(data_amount, tiempoProcesos, "o-")
    plt.xlabel('Cantidad de datos procesados') 
    plt.ylabel('Tiempo(s)') 
    plt.title('Grafica comparacion computo secuancial vs paralelo')
    plt.legend(["Secuencial","Paralela"])    
    plt.savefig("grafica.jpg")


if __name__ == "__main__":
    main()