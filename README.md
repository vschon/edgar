
'''
CORE PROCEDURE:

    Part 0. Identifier convert and data preparation 
    1. CUSIP to ticker
    2. ticker to CIK
    3. check all data is in position

    Part 1. Data cleaning:
        1. raw data:
            - rs:   daily stock return
            - rp:   daily portfolio return
            - sue:  quarterly SUE
            - F10:  quarterly report
        2. Transform data into structure data:

        Database structure:
        /
            /database
                /section1
                    /symbol1
                        /symbol1_date1.txt
                            symbol: A
                            fileDate:        2013.01.04
                            windowBegin:        2013.01.04
                            windowEnd:          2013.01.06
                            windowReturn:       0.02
                            windowPortReturn:   0.003
                            windowExcessReturn: 0.017
                            label               1:Rise 2:Flat(-0.003<excessret<0.003) 3:Fall 
                            SUE:                0.01
                            WORDLIST:
                            word1:count1
                            ...
                        /symbol1_date2.txt
                        ...
                    /symbol2
                    ...
                /section2
                ...
                /section10

        ALGO: GENERATE DATABASE
        window = 2 trading_days

        for symbol in section_shortlist
            get indexfile of symbol
            read the index file into dataframe
            for each entry >= 2000 with 10-K or 10-Q
                filedate = DEF_getfiledate(form)
                windowBegin, windowEnd, windowReturn = DEF_getWindow(symbol, window, filedate)
                windowPortReturn = DEF_getPortfolioReturn(windowBegin, windowEnd, section)
                windowExcessReturn = windowReturn - windowPortReturn
                SUE = DEF_getSUE(symbol, filedate)
                BOW = DEF_generateBOW(form)

                store to section/symbol/symbol_date.txt

        ALGO: GENERATE BOW

    Part 2. TRAINING SET DEVELOPMENT

    cutDate: data before cutDate are used as training data

    Part 3: UTILITY FUNCTION
    1. Data Iterator
        return label, sue, bow
    2. Get all counts 
    3. Generate thined dictionary
    


    Part 3. USE SVM, NB TO TRAIN AND TEST 


'''
