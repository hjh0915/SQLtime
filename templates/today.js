<script>
    $().ready(function () {         
    Highcharts.chart('lineChart', { 
        chart: {
            type: 'line'
        },
        title: {
            text: '每30分钟流入流出金额'
        },
        subtitle: {
            text: ''
        },
        xAxis: {
            categories: {{ gs_time|safe }},
            labels: {
            y : 20,
            rotation: -45,
            align: 'right' 
            }
        },
        yAxis: {
            min: 0,
            title: {
                text: '流入流出金额'
            }
        },
        tooltip: {
            formatter: function() {
                var msg = '';
  
                msg = msg + '<div><b>' + this.x + '</b></div>';
                msg = msg + '<b>[' + this.series.name + '] ' + this.y + '</b>';
                msg = msg + '<div>';
                if (this.y != 0.0) {
                  var dtl = this.point.details;
                  for (var i in dtl) {
                    msg = msg + '<div>';
                    msg = msg + dtl[i].rpt_sum + ':' + dtl[i].amt + ' ';
                    msg = msg + '</div>';
                  }
                }
                msg = msg + '</div>';
                return msg;
            },
            useHTML: true
        },
        plotOptions: {
            line: {
            lineWidth: 3,
            marker: {
                enabled: false,
                radius: 1
            },
            shadow: false,
            states: {
                hover: {
                lineWidth: 2
                }
            },
            threshold: null
            }
        },
        credits: {
            enabled: false
        },
        series: [{
            name: '流入',
            data: {{ gs_amt_in|safe }}
        },{
            name: '流出',
            data: {{ gs_amt_out|safe }}
        }]
    });
    });
</script>

<script>
    $().ready(function () {         
    Highcharts.chart('areaChart', { 
        chart: {
            type: 'area'
        },
        title: {
            text: '每30分钟流入流出金额累计量'
        },
        subtitle: {
            text: ''
        },
        xAxis: {
            categories: {{ gs_time|safe }},
            labels: {
                y : 20,
                rotation: -45,
                align: 'right' 
            }
        },
        yAxis: {
            min: 0,
            title: {
                text: '流入流出金额'
            }
        },
        tooltip: {
            // head + 每个 point + footer 拼接成完整的 table
            headerFormat: '<span style="font-size:10px">{point.key}</span><table>',
            pointFormat: '<tr><td style="color:{series.color};padding:0">{series.name}: </td>' +
            '<td style="padding:0"><b>{point.y:1.2f}</b></td></tr>',
            footerFormat: '</table>',
            shared: true,
            useHTML: true
        },
        plotOptions: {
            area: {
            marker: {
                enabled: false,
                symbol: 'circle',
                radius: 2,
                states: {
                hover: {
                    enabled: true
                }
                }
            }
            }
        },
        credits: {
            enabled: false
        },
        series: [{
            name: '流入',
            data: {{ gs_sum_in|safe }}
        },{
            name: '流出',
            data: {{ gs_sum_out|safe }}
        }]
        });
    });
</script>