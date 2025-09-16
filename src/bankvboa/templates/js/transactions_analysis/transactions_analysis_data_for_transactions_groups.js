var data_evolution = {};
var accumulated_data_evolution = {};

{% for event_uuid in event_uuids %}
{% set event = data["events"][event_uuid] %}
{% set values = event|get_values([{"name": {"filter": "amount","op": "=="}, "group":"amount"}, {"name": {"filter": "group","op": "=="}, "group":"group"}]) %}

{# Obtain amount #}
{% set amount = None %}
{% if "amount" in values %}
{% set amount = values["amount"][0]["value"] %}
{% endif %}

{# Obtain group #}
{% set group = None %}
{% if "group" in values %}
{% set group = values["group"][0]["value"] %}
{% endif %}

var group = "{{ group }}"
var amount = Math.abs(parseFloat("{{ amount }}"));

/* Add values to the data structure */
var date = "{{ event.start }}";
var id = "{{ event_uuid }}";

if (!("groups_evolution" in data_evolution)){
    data_evolution["groups_evolution"] = [];
}

if (!("accumulated_groups_evolution" in data_evolution)){
    data_evolution["accumulated_groups_evolution"] = [];
}

if (!(group in accumulated_data_evolution)){
    accumulated_data_evolution[group] = 0;
}

data_evolution["groups_evolution"].push({
    "id": id,
    "group": group,
    "x": date,
    "y": amount,
    "tooltip": get_tooltip(id, date, group, "{{ amount }}")
});

// Compute evolution of the account
accumulated_data_evolution[group] += amount;
data_evolution["accumulated_groups_evolution"].push({
    "id": "accumulated-evolution-" + id,
    "group": group,
    "x": date,
    "y": accumulated_data_evolution[group],
    "tooltip": get_tooltip(id, date, group, accumulated_data_evolution[group])
});

{% endfor %}

/* Function to obtain the text for the tooltip of the widgets */
function get_tooltip(id, date, group, amount){
    
    var tooltip = "<table border='1'>" +
        "<tr><td>ID</td><td><a href='/eboa_nav/query-event-links/" + id + "'>" + id + "</a></td></tr>" +
        "<tr><td>Date</td><td>" + date + "</td></tr>" +
        "<tr><td>Group</td><td>" + group + "</td></tr>" +
        "<tr><td>Amount</td><td>" + amount + "</td></tr>" +
        "</table>"

    return tooltip
};
