var data_evolution = {};
var account_evolution = 0
var incoming_evolution = 0
var spending_evolution = 0

{% for event_uuid in event_uuids %}

{% set event = data["events"][event_uuid] %}
{% set values = event|get_values([{"name": {"filter": "amount","op": "=="}, "group":"amount"}, {"name": {"filter": "balance","op": "=="}, "group":"balance"}, {"name": {"filter": "concept","op": "=="}, "group":"concept"}]) %}

{% set groups_values = event|get_values([{"name": {"filter": "group.","op": "regex"}, "group":"groups"}], False) %}
{% set entities_values = event|get_values([{"name": {"filter": "entity.","op": "regex"}, "group":"entities"}], False) %}

{# Obtain amount #}
{% set amount = None %}
{% if "amount" in values %}
{% set amount = values["amount"][0]["value"] %}
{% endif %}

{# Obtain balance #}
{% set balance = None %}
{% if "balance" in values %}
{% set balance = values["balance"][0]["value"] %}
{% endif %}

{# Obtain concept #}
{% set concept = None %}
{% if "concept" in values %}
{% set concept = values["concept"][0]["value"] %}
{% endif %}

{# Obtain groups #}
{% set groups = None %}
{% if "groups" in groups_values %}
{% set groups = groups_values["groups"]|map(attribute="value")|join(", ") %}
{% endif %}

/* Add values to the data structure */
var date = "{{ event.start }}";
var id = "{{ event_uuid }}";
var amount = parseFloat("{{ amount }}");
var balance = parseFloat("{{ balance }}");

if (!("transactions_incoming" in data_evolution)){
    data_evolution["transactions_incoming"] = [];
}

if (!("incoming_evolution" in data_evolution)){
    data_evolution["incoming_evolution"] = [];
    incoming_evolution = balance - amount;
    
}

if (!("transactions_spending" in data_evolution)){
    data_evolution["transactions_spending"] = [];
}

if (!("spending_evolution" in data_evolution)){
    data_evolution["spending_evolution"] = [];
    spending_evolution = balance - amount;
}

if (!("account_evolution" in data_evolution)){
    data_evolution["account_evolution"] = [];
    account_evolution = balance - amount;
}

var group = "transactions_spending";
var group_leyend = "Spending transactions";
if (amount > 0){
    group = "transactions_incoming";
    group_leyend = "Incoming transactions";

    // Compute incoming evolution
    incoming_evolution += amount
    data_evolution["incoming_evolution"].push({
        "id": id,
        "group": "Evolution of incoming",
        "x": date,
        "y": incoming_evolution,
        "tooltip": get_tooltip(id, date, "{{ groups }}", incoming_evolution, "{{ concept }}", "{{ balance }}")
    });
    
}
else{

    // Compute spending evolution
    spending_evolution += amount
    data_evolution["spending_evolution"].push({
        "id": id,
        "group": "Evolution of spending",
        "x": date,
        "y": spending_evolution,
        "tooltip": get_tooltip(id, date, "{{ groups }}", spending_evolution, "{{ concept }}", "{{ balance }}")
    });
}
data_evolution[group].push({
    "id": id,
    "group": group_leyend,
    "x": date,
    "y": "{{ amount }}",
    "tooltip": get_tooltip(id, date, "{{ groups }}", "{{ amount }}", "{{ concept }}", "{{ balance }}")
});

// Compute evolution of the account
account_evolution += amount;
data_evolution["account_evolution"].push({
    "id": "account-evolution-" + id,
    "group": "Account evolution",
    "x": date,
    "y": account_evolution,
    "tooltip": get_tooltip(id, date, "{{ groups }}", account_evolution, "{{ concept }}", "{{ balance }}")
});

{% endfor %}

/* Function to obtain the text for the tooltip of the widgets */
function get_tooltip(id, date, groups, amount, concept, balance){
    
    var tooltip = "<table border='1'>" +
        "<tr><td>ID</td><td><a href='/eboa_nav/query-event-links/" + id + "'>" + id + "</a></td></tr>" +
        "<tr><td>Date</td><td>" + date + "</td></tr>" +
        "<tr><td>Groups</td><td>" + groups + "</td></tr>" +
        "<tr><td>Amount</td><td>" + amount + "</td></tr>" +
        "<tr><td>Concept</td><td>" + concept + "</td></tr>" +
        "<tr><td>Balance</td><td>" + balance + "</td></tr>" +
        "</table>"

    return tooltip
};
