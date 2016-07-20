# Distributed Systems Facebook Analysis

This project....


## Structure
```
├── facebookanalysis
│   ├── fbparser
│   │   ├── fbparser.py
│   │   ├── messages.htm
│   │   ├── README.md
│   │   └── requirements.txt
│   ├── README.md
│   └── wc.go
├── graphbuilder
│   └── graphbuilder.go
└── mapreduce
    ├── common.go
    ├── mapreduce.go
    ├── master.go
    ├── test_test.go
    └── worker.go
```


## Useage:

1. Preprocess your facebook data to create input data file; see ```fbparser/README```

2. Use MapReduce to create prefix trees of all your conversations; One file will be produced per person. Run ```go run wc.go master <PARSED_INPUT_FILE.txt> sequential```

3. Generate a stacked bar graph via: ```go run wc.go graph stack-hist mrtmp.<PARSED_INPUT_FILE.txt> <Number of friends to evaluate>```

	* Note: E.g., 'Number of friends' to evalutate is usually 3

4. Cleanup temp files via: ```./cleanup.sh```

5. View results in: FabulousFacebookFriends.png
