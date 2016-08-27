from bokeh.core.properties import StringSpec, NumberSpec, Instance
from bokeh.models import Widget, ColumnDataSource, DataSource

IMPLEMENTATION = """
$ = require "jquery"
p = require "core/properties"
Widget = require "models/widgets/widget"
ColumnDataSource = require "models/sources/column_data_source"

class ProgressBarView extends Widget.View
    initialize: (options) ->
        super(options)
        @render()
        @listenTo(@model, 'change', @render)
        @listenTo(@model.source, 'change', @render)

    render: () ->
        super()
        @$el.empty()

        data = @model.source.data
        $bars = $("<ul></ul>")
        for i in [0...data.name.length]
            console.log(i)
            $bar = $("
                <li>
                    #{data.name[i]} - #{data.done[i]} / #{data.all[i]}
                </li>
            ")
            $bars.prepend($bar)
        @$el.html($bars)
        return @

class ProgressBar extends Widget.Model
    type: "ProgressBar"
    default_view: ProgressBarView
    @define {
        #function: [ p.StringSpec, {field: 'name'} ]
        #all: [ p.NumberSpec, {field: 'all'} ]
        #done: [p.NumberSpec, {field: 'done'} ]
        source: [ p.Instance, () -> new ColumnDataSource.Model() ]
    }
module.exports =
    Model: ProgressBar
"""


class ProgressBar(Widget):
    """
    A custom Dask progress bar
    """
    __implementation__ = IMPLEMENTATION

    source = Instance(DataSource, default=lambda: ColumnDataSource(), help="""
        Data source for progress bar
    """)
    #
    # Prevent any configuration for now
    #
    #function = StringSpec(default='name', help="""
    #    The name of the function
    #""")
    #all = NumberSpec(default='all', help="""
    #    The total number of calls to be made.
    #""")
    #done = NumberSpec(default='done', help="""
    #    The total number of calls made.
    #""")
