import matplotlib.pyplot as plt


def visulation_df(df=None, x_axis=None, y_axis=None,
                  x_label=None, y_label=None, title=None, sorting='y_axis'):
    if sorting == 'y_axis':
        sorting = y_axis
    else:
        sorting = x_axis
    plot_test = df.sort_values([sorting], ascending=True)
    plot_test.plot(figsize=(20, 10), kind="bar", color="red",
                   x=x_axis, y=y_axis, legend=False, sort_columns=True)
    plt.xlabel(x_label, fontsize=18)
    plt.ylabel(y_label, fontsize=18)
    plt.title(title, fontsize=28)
    plt.xticks(size=18)
    plt.yticks(size=18)
    plt.show()
