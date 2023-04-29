/*
 * Simson L. Garfinkel
 * 2020-10-29 - Client side support for PM
 *
 * If the page dvs.html is entered with ?q=<foo>, then put <foo> into ths earch field and do the search.
 */



const DBR_STATS_URL='api/dbrecon';
const SELECT_TIMEOUT = 60*1000; // 60 seconds
const SCROLL_TIME = 500;        // 0.5 seconds

const cat = (accumulator, currentValue) => accumulator + currentValue;

function make_th (d) { return `<th>${d}</th>`;}
function make_td (d) { return `<td>${d}</td>`;}
function make_tr (d) { return `<tr>${d}</tr>\n`;}

function dict_to_th(d) {
    return make_tr( Object.keys(d).map(make_th).reduce(cat) );
}

function dict_to_td(d) {
    return make_tr( Object.values(d).map(make_td).reduce(cat) );
}

var my;

$(document).ready(function() {
    console.log("dbrecon.js");
    $.ajaxSetup({timeout:SELECT_TIMEOUT});

    // See if we got a search request
    let requested_reident= getParams()["reident"];

    $.get( DBR_STATS_URL, function(data, status) {
        if (status == 'success' ){
            data = JSON.parse(data);
            my = data; // for debugging
            console.log("data=",data);
            let results = $("#results");
            if (!requested_reident){
                results.append("<h2>Available REIDENTs:</h2>\n<ul>\n");
            }
            for (var i =0;i<data.queries.length; i++){
                 var reident = data.queries[i][0];
                 var tables  = data.queries[i][1];

                if (!requested_reident){
                    results.append(`<li><a href='dbrecon.html?reident=${reident}'>${reident}</a></li>\n`);
                }
                else if (reident==requested_reident) {
                    $("#results").append( "<h2> REIDENT "+reident + "</h2>");
                    for (var t =0; t<tables.length; t++){
                        var name = tables[t][0];
                        var rows = tables[t][1];
                        console.log("t=",t,"rows=",rows);
                        if ( rows && rows.length>0) {
                            results.append( `<h3>${name} </h3> <table class='sql usa-table'> ${dict_to_th(rows[0]) + rows.map(dict_to_td).reduce(cat)} </table>`);
                        }
                    }
                }
            }
        }
    });

});
