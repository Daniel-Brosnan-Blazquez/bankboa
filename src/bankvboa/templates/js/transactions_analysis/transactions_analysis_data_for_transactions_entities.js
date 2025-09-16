var data_evolution = {};
var accumulated_data_evolution = {};

{% for event_uuid in event_uuids %}
{% set event = data["events"][event_uuid] %}
{% set values = event|get_values([{"name": {"filter": "amount","op": "=="}, "group":"amount"}, {"name": {"filter": "entity","op": "=="}, "group":"entity"}]) %}

{# Obtain amount #}
{% set amount = None %}
{% if "amount" in values %}
{% set amount = values["amount"][0]["value"] %}
{% endif %}

{# Obtain entity #}
{% set entity = None %}
{% if "entity" in values %}
{% set entity = values["entity"][0]["value"] %}
{% endif %}

var entity = "{{ entity }}"
var amount = Math.abs(parseFloat("{{ amount }}"));

/* Add values to the data structure */
var date = "{{ event.start }}";
var id = "{{ event_uuid }}";

if (!("entities_evolution" in data_evolution)){
    data_evolution["entities_evolution"] = [];
}

if (!("accumulated_entities_evolution" in data_evolution)){
    data_evolution["accumulated_entities_evolution"] = [];
}

if (!(entity in accumulated_data_evolution)){
    accumulated_data_evolution[entity] = 0;
}

data_evolution["entities_evolution"].push({
    "id": id,
    "group": entity,
    "x": date,
    "y": amount,
    "tooltip": get_tooltip(id, date, entity, "{{ amount }}")
});

// Compute evolution of the account
accumulated_data_evolution[entity] += amount;
data_evolution["accumulated_entities_evolution"].push({
    "id": "accumulated-evolution-" + id,
    "group": entity,
    "x": date,
    "y": accumulated_data_evolution[entity],
    "tooltip": get_tooltip(id, date, entity, accumulated_data_evolution[entity])
});

{% endfor %}

/* Function to obtain the text for the tooltip of the widgets */
function get_tooltip(id, date, entity, amount){
    
    var tooltip = "<table border='1'>" +
        "<tr><td>ID</td><td><a href='/eboa_nav/query-event-links/" + id + "'>" + id + "</a></td></tr>" +
        "<tr><td>Date</td><td>" + date + "</td></tr>" +
        "<tr><td>Entity</td><td>" + entity + "</td></tr>" +
        "<tr><td>Amount</td><td>" + amount + "</td></tr>" +
        "</table>"

    return tooltip
};
