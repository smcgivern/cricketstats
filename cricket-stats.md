#!/bin/bash
# make spaces underscores in each csv file
find *.csv -exec rename 's/\s/_/g' {} \;

# make all files lowercase
find *.csv -exec rename 'y/A-Z/a-z/' {} \;

# do the same to the file line in each csv file
# We run two commands to sed, the first one:
1s/.*/\L&/ < on the first line, make sure all characters are lowercase \L
1s/ /_/g ... on the first line, replace all spaces with underscores, globally
find *.csv -exec sed -i '1s/.*/\L&/;1s/ /_&/' {} \;

# get the first line from the file
# so our data is in groups of 3 files for some reason, and there are repeated results of data.
# let's get the first line and make them 

# SELECT * 
# FROM "men_odi_player_innings_stats" 
# WHERE innings_player="SPD Smith" AND innings_runs_scored is not NULL 
# ORDER BY innings_date;

Bloody hell, our data has 3 results of the same bloody query. Looks like we're going to make them unique.

To make our files unique, we can be boring and do it in SQL, and modify our original CSV files the fast way, and then reimport them.

While we're doing that we might as well combine the data from different centuries, because why differentiate based on a human construct such as years, though sport is also a human construct, so why am I doing this anyway... 

First we need to get the first line, which is our column headings.

HEADER=$(head -n 1 men_test_team_match_results_-_19th_century.csv)

Them merge the files, while deleting the header from each of the lines. 
find . -name "men_test_team_match_results_-_*.csv" | xargs -n 1 tail -n +2 > men_test_player_innings_all.csv

Okay, so now we have all the test teams merged, removed the first line which is our columns, next step is to make sure all the lines are unique.

The `sort` command is really handy for this. We're going to sort our file by a particular column, and pass in the -u flag to make sure each line is unique.

```bash
sort -k1 -u men_test_player_innings_all.csv > men_test_player_innings_all.csv
```

Then finally, we can add the header back in,

```bash
sed -i '1 i $HEADER' men_test_player_innings_all.csv
```

Let's just sanity check that that worked. 

```bash
head -n 5 men_test_player_innings_all.csv
```

Wooo!

Okay, so now we've gotten to the point where we hoped we would have started, clean data, I guess. How do we know we haven't fucked something up? 

Well, Don Bradman's test batting average was 99.94, famously, how about we write a function to check the average is correct. 

SQL is feeling a little bit lame, honestly, so how about we stick to what we know and write a function to get the average.

Ao the average of a batter is the number of runs scored in their career minus the number of times they have been dismissed.

So we can write a function for each.

```python
def get_total_runs(player):
    return df.loc[df['innings_player'] == player, 'innings_runs_scored_num'].dropna().astype(int).sum()

def times_dismissed(player):
    num_innings = len(df.loc[df['innings_player'] == player, 'innings_runs_scored_num'].dropna())
    not_out = df.loc[df['innings_player'] == player, 'innings_not_out_flag'].dropna().astype(int).sum()
    return num_innings - not_out
```
So we can just combine these two to get the average.

```python
def get_average(player):
    runs = get_total_runs(player)
    dismissed = times_dismissed(player)
    return runs / dismissed
```

```python
get_average('DG Bradman')
Out[85]: 99.94285714285714
```

Fuckn yeh boooooiiiii!

That was fun an all, but really, what we want is to do some fucking number crunching here. We can calculate the average of every player in the history of the game.

First we want to get all the players...

```python
all_players = df.innings_player.drop_duplicates()
```
That will just give us a pandas Series, but I want to make it into a DataFrame and add the column for the average for each player

```python
all_players = pd.DataFrame(all_players)
```
You can use the `apply` function to apply the function to all values, who'd have thought that's what it would do?

all_players['average'] = all_players.innings_player.apply(get_average)

After that we should filter out the infinite and not-a-number results

all_players.replace([np.inf, -np.inf], np.nan).dropna()

Next we can displayer the results from highest to lowest. To reveal the best batsman of all time....

all_players.sort_values(by=['average'], ascending=False)
In [94]: all_players.sort_values(by=['average'], ascending=False)
Out[94]: 
      innings_player     average
5200    KR Patterson  144.000000
60752   AG Ganteaume  112.000000
48626       Abid Ali  107.000000
13993     DG Bradman   99.942857
52126       MN Nawaz   99.000000
...              ...         ...
79345      RGM Patel    0.000000
79442        R Singh    0.000000
23384       SP Davis    0.000000
23026    RHD Sellers    0.000000
29331        GG Hall    0.000000


Holy fuck. This can't be fucking real.


It fucking is! 

Okay, so why the hell am I doing this anyway, using the power of programming to answer questions that cannot be answered with normal trawling. The [statsguru search]() is actually pretty good. But there are a bunch of things that it can't do. We can't be constrained by what others want you to see, we can fucking do it ourselves. 

Now that we've loaded the entire test history of data into our little laptop, we can see what shit can we uncover.

So, I have a few goals, actually they're not my goals, but say we want to get the most consecutive scores greater than `n`. Let's give it a shot.

So when we consider this problem, we need to sort all the results by date, and then for each player, count the number of scores each player has greater than that number in a row, and whoever has the maximum is it, print that shit.

Nerds out there, I don't particularly care about O(n) efficiency in this, there's a nice hole in the O which describes where you can shove your 'efficient' algorithm.

There's a really easy way to count the number of scores for each player greater than a given num (not in a row).

```python
def get_all_player_innings(player):
    return df.loc[df['innings_player'] == player, 'innings_runs_scored_num'].dropna().astype(int)
```

Let's see how many ducks The Don actually made.

```python
don = get_all_player_innings('DG Bradman')

don[don == 0].count()
7
```

Actually, that's how many times he was on 0, it doesn't account for any 0-not-outs. We should fix how we do these things.

```python
def get_all_player_stats(player):
    # return the entire dataframe but filtered for that player
    return df.loc[df['innings_player'] == player]

def num_ducks(player):
    all_stats = get_all_player_stats(player)
    # so the innings_runs_scored_num is currently saved as a fucking string, I know.
    ducks = all_stats[(all_stats.innings_not_out_flag == 0) & (all_stats.innings_runs_scored_num == '0')]
    return len(ducks)
```


```python
num_ducks('DG Bradman')
7
```
So that was pretty easy, filtering with conditions is not that hard. But what about *consecutive* scores?

When I downloaded the database, it was sorted by high-score. A better way, probably in general, is to sort by date.

First we'll convert the to actual dates instead of text.
```python
df.innings_date = pd.to_datetime(df.innings_date, infer_datetime_format=True)
```
then sort, we've done this before.
```python
df = df.sort_values(by=['innings_date'])
```
So this is where things get a little tricky, for a player, we just want *consecutive* scores above a certain value. Looks like we're actually going to convert the numbers column to be actual numbers. 

```python
df.innings_runs_scored_num = pd.to_numeric(df.innings_runs_scored_num, errors='coerce').astype("Int32")
```

This creates a *nullable integer data type*, which I've never used before, but it allows integers to be mixed with NaNs.

Okay, so for each player, in innings sorted by date, lets find the most consecutive scores over `n`. This is actually a really hard problem, what we really want to do is mark if each score matches the condition we're after, *then* get the maximum number of those in a row.

To do this, we're going to use our 'all_players' dataframe (), and add columns to that, that match the condition we're after.

First, we're going to take our original data, and add a column `True` or `False`, is each innings greater than 40.

Then, for each player, count the number of those in a row, and add that to our `all_players` dataframe.

