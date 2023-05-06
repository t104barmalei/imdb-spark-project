from tasks.task1 import TaskOne
from tasks.task2 import TaskTwo
from tasks.task3 import TaskThree
from tasks.task4 import TaskFour
from tasks.task5 import TaskFive
from tasks.task6 import TaskSix
from tasks.task7 import TaskSeven
from tasks.task8 import TaskEight


def main(show=False, output_path=None):
    try:
        names_ua = TaskOne(path='imdb_data/title.akas.tsv.gz')
        result1 = names_ua.get_data()
        if show:
            names_ua.show_table(result1)
    except:
        print('Res1 error')

    try:
        people_names_born19 = TaskTwo(path='imdb_data/name.basics.tsv.gz')
        result2 = people_names_born19.get_data()
        if show:
            people_names_born19.show_table(result2)
    except:
        print('Res2 error')

    try:
        runtime3 = TaskThree(path='imdb_data/title.basics.tsv.gz')
        result3 = runtime3.get_data()
        if show:
            runtime3.show_table(result3)
    except:
        print('Res3 error')

    try:
        df_temp = TaskFour(path='imdb_data/name.basics.tsv.gz',path1='imdb_data/title.principals.tsv.gz', \
                           path2='imdb_data/title.akas.tsv.gz')
        result4 = df_temp.get_data()
        if show:
            df_temp.show_table(result4)
    except:
        print('Res4 error')

    try:
        df_temp = TaskFive(path='imdb_data/title.akas.tsv.gz', path1='imdb_data/title.basics.tsv.gz', \
                          path2='imdb_data/title.ratings.tsv.gz')
        result5 = df_temp.get_data()
        if show:
            df_temp.show_table(result5)
    except:
        print('Res5 error')

    try:
        df_temp = TaskSix(path='imdb_data/title.basics.tsv.gz', films_episode_df_path='imdb_data/title.episode.tsv.gz')

        result6 = df_temp.get_data()
        if show:
            df_temp.show_table(result6)
    except:
        print('Res6 error')

    try:
        df_temp = TaskSeven(path='imdb_data/title.basics.tsv.gz', films_rating_path='imdb_data/title.ratings.tsv.gz')

        result7 = df_temp.get_data()
        if show:
            df_temp.show_table(result7)
    except:
        print('Res7 error')

    try:
        df_temp = TaskEight(path='imdb_data/title.basics.tsv.gz', films_rating_path='imdb_data/title.ratings.tsv.gz')

        result8 = df_temp.get_data()
        if show:
            df_temp.show_table(result8)
    except:
        print('Res8 error')


if __name__ == "__main__":
    main(show=True, output_path=None)

