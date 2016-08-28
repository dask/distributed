#from bokeh.core.properties import StringSpec, NumberSpec,
from bokeh.core.properties import Instance
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
        $bars = $("<ul class='grid'></ul>")
        for i in [0...data.name.length]
            done = data.done[i]
            all = data.all[i]
            percent = parseInt(done/all * 100)

            # Can use bar_class to set other colors
            if percent == 100
                bar_class = 'bar finished'
            else
                bar_class = 'bar'

            $bar = $("
                <li class='grid-item'>
                    <div class='description'>
                        <span class='function-name'>#{data.name[i]}</span>
                        <span class='count'>#{done}/#{all}</span>
                    </div>
                    <div class='meter'>
                        <span class='#{bar_class}' style='width: #{percent}%'></span>
                    </div>
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
