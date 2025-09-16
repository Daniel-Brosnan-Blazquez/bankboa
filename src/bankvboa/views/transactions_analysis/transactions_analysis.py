"""
Definition of the transactions analysis view

Written by Daniel Brosnan Blázquez

module bankvboa
"""
# Import python utilities

# Import flask utilities
from flask import Blueprint, flash, g, current_app, redirect, render_template, request, url_for
from flask_debugtoolbar import DebugToolbarExtension
from flask import jsonify

# Import vboa security
from vboa.security import auth_required, roles_accepted

# Import vboa views functions
from vboa import functions as vboa_functions

# Import eboa utilities
from eboa.engine.query import Query
from eboa.engine import export as eboa_export

bp = Blueprint("transactions_analysis", __name__, url_prefix="/views")
query = Query()

version = "1.0"

@bp.route("/transactions-analysis", methods=["GET", "POST"])
@auth_required()
@roles_accepted("administrator", "service_administrator", "operator", "analyst", "operator_observer", "observer")
def show_transactions_analysis():
    """
    Transactions analysis view for the BANKBOA project.
    """
    current_app.logger.debug("Transactions analysis view")

    # Initialize reporting period (now - 30 days, now)
    window_size = 30
    window_delay = 30

    filters = {}
    if request.method == "POST":
        filters = request.form.to_dict(flat=False).copy()
    # end if

    start_filter, stop_filter = vboa_functions.get_start_stop_filters(filters, window_size, window_delay)

    # Build data dictionary
    data = {}
    
    data["metadata"] = {}
    metadata = data["metadata"]
    metadata["version"] = version
    metadata["reporting_start"] = stop_filter["date"]
    metadata["reporting_stop"] = start_filter["date"]
    
    #####
    # Query events
    #####
    # Movement events
    movement_events = query.get_events(gauge_names = {"filter": "MOVEMENT", "op": "=="},
                                                                     stop_filters = [{"date": metadata["reporting_start"], "op": ">"}],
                                                                     start_filters = [{"date": metadata["reporting_stop"], "op": "<"}],
                                                                     order_by = {"field": "start", "descending": False})
    
    eboa_export.export_events(data, movement_events, group = "movement_events", include_ers = False, include_annotations = False, include_alerts = True)

    # Aggregated movements per group and per month events
    aggregated_movements_group_month_events = query.get_events(gauge_names = {"filter": "AGGREGATED_MOVEMENTS_GROUP_MONTH", "op": "=="},
                                                                     stop_filters = [{"date": metadata["reporting_start"], "op": ">"}],
                                                                     start_filters = [{"date": metadata["reporting_stop"], "op": "<"}],
                                                                     order_by = {"field": "start", "descending": False})
    
    eboa_export.export_events(data, aggregated_movements_group_month_events, group = "aggregated_movements_group_month_events", include_ers = False, include_annotations = False, include_alerts = True)

    # Aggregated movements per entity and per month events
    aggregated_movements_entity_month_events = query.get_events(gauge_names = {"filter": "AGGREGATED_MOVEMENTS_ENTITY_MONTH", "op": "=="},
                                                                     stop_filters = [{"date": metadata["reporting_start"], "op": ">"}],
                                                                     start_filters = [{"date": metadata["reporting_stop"], "op": "<"}],
                                                                     order_by = {"field": "start", "descending": False})
    
    eboa_export.export_events(data, aggregated_movements_entity_month_events, group = "aggregated_movements_entity_month_events", include_ers = False, include_annotations = False, include_alerts = True)

    # Aggregated movements per year events
    aggregated_movements_group_year_events = query.get_events(gauge_names = {"filter": "AGGREGATED_MOVEMENTS_GROUP_YEAR", "op": "=="},
                                                                     stop_filters = [{"date": metadata["reporting_start"], "op": ">"}],
                                                                     start_filters = [{"date": metadata["reporting_stop"], "op": "<"}],
                                                                     order_by = {"field": "start", "descending": False})
    
    eboa_export.export_events(data, aggregated_movements_group_year_events, group = "aggregated_movements_group_year_events", include_ers = False, include_annotations = False, include_alerts = True)

    # Aggregated movements per entity and per year events
    aggregated_movements_entity_year_events = query.get_events(gauge_names = {"filter": "AGGREGATED_MOVEMENTS_ENTITY_YEAR", "op": "=="},
                                                                     stop_filters = [{"date": metadata["reporting_start"], "op": ">"}],
                                                                     start_filters = [{"date": metadata["reporting_stop"], "op": "<"}],
                                                                     order_by = {"field": "start", "descending": False})
    
    eboa_export.export_events(data, aggregated_movements_entity_year_events, group = "aggregated_movements_entity_year_events", include_ers = False, include_annotations = False, include_alerts = True)

    return render_template("views/transactions_analysis/transactions_analysis.html", data=data)

@bp.route("/group-analysis")
@auth_required()
@roles_accepted("administrator", "service_administrator", "operator", "analyst", "operator_observer", "observer")
def show_group_analysis():
    """
    Group analysis view for the BANKBOA project.
    """
    current_app.logger.debug("Group analysis view")

    # Build data dictionary
    data = {}
    
    data["metadata"] = {}
    metadata = data["metadata"]
    metadata["version"] = version
    metadata["reporting_start"] = request.args.get("reporting_start")
    metadata["reporting_stop"] = request.args.get("reporting_stop")
    metadata["group"] = request.args.get("group")
    
    #####
    # Query events
    #####
    # Movement events
    movement_events = query.get_events(gauge_names = {"filter": "MOVEMENT", "op": "=="},
                                       stop_filters = [{"date": metadata["reporting_start"], "op": ">"}],
                                       start_filters = [{"date": metadata["reporting_stop"], "op": "<"}],
                                       value_filters = [{"name": {"filter": "group%", "op": "like"}, "type": "text", "value": {"op": "==", "filter": metadata["group"]}}],
                                       order_by = {"field": "start", "descending": False})
    
    eboa_export.export_events(data, movement_events, group = "movement_events", include_ers = False, include_annotations = False, include_alerts = True)

    # Aggregated movements per group and per month events
    aggregated_movements_group_month_events = query.get_events(gauge_names = {"filter": "AGGREGATED_MOVEMENTS_GROUP_MONTH", "op": "=="},
                                                                stop_filters = [{"date": metadata["reporting_start"], "op": ">"}],
                                                                start_filters = [{"date": metadata["reporting_stop"], "op": "<"}],
                                                                value_filters = [{"name": {"filter": "group", "op": "=="}, "type": "text", "value": {"op": "==", "filter": metadata["group"]}}],
                                                                order_by = {"field": "start", "descending": False})
    
    eboa_export.export_events(data, aggregated_movements_group_month_events, group = "aggregated_movements_group_month_events", include_ers = False, include_annotations = False, include_alerts = True)

    # Aggregated movements per year events
    aggregated_movements_group_year_events = query.get_events(gauge_names = {"filter": "AGGREGATED_MOVEMENTS_GROUP_YEAR", "op": "=="},
                                                               stop_filters = [{"date": metadata["reporting_start"], "op": ">"}],
                                                               start_filters = [{"date": metadata["reporting_stop"], "op": "<"}],
                                                               value_filters = [{"name": {"filter": "group", "op": "=="}, "type": "text", "value": {"op": "==", "filter": metadata["group"]}}],
                                                               order_by = {"field": "start", "descending": False})
    
    eboa_export.export_events(data, aggregated_movements_group_year_events, group = "aggregated_movements_group_year_events", include_ers = False, include_annotations = False, include_alerts = True)

    return render_template("views/transactions_analysis/transactions_analysis.html", data=data)


@bp.route("/entity-analysis")
@auth_required()
@roles_accepted("administrator", "service_administrator", "operator", "analyst", "operator_observer", "observer")
def show_entity_analysis():
    """
    Entity analysis view for the BANKBOA project.
    """
    current_app.logger.debug("Entity analysis view")

    # Build data dictionary
    data = {}
    
    data["metadata"] = {}
    metadata = data["metadata"]
    metadata["version"] = version
    metadata["reporting_start"] = request.args.get("reporting_start")
    metadata["reporting_stop"] = request.args.get("reporting_stop")
    metadata["entity"] = request.args.get("entity")
    
    #####
    # Query events
    #####
    # Movement events
    movement_events = query.get_events(gauge_names = {"filter": "MOVEMENT", "op": "=="},
                                       stop_filters = [{"date": metadata["reporting_start"], "op": ">"}],
                                       start_filters = [{"date": metadata["reporting_stop"], "op": "<"}],
                                       value_filters = [{"name": {"filter": "entity%", "op": "like"}, "type": "text", "value": {"op": "==", "filter": metadata["entity"]}}],
                                       order_by = {"field": "start", "descending": False})
    
    eboa_export.export_events(data, movement_events, group = "movement_events", include_ers = False, include_annotations = False, include_alerts = True)

    # Aggregated movements per entity and per month events
    aggregated_movements_entity_month_events = query.get_events(gauge_names = {"filter": "AGGREGATED_MOVEMENTS_ENTITY_MONTH", "op": "=="},
                                                                stop_filters = [{"date": metadata["reporting_start"], "op": ">"}],
                                                                start_filters = [{"date": metadata["reporting_stop"], "op": "<"}],
                                                                value_filters = [{"name": {"filter": "entity", "op": "=="}, "type": "text", "value": {"op": "==", "filter": metadata["entity"]}}],
                                                                order_by = {"field": "start", "descending": False})
    
    eboa_export.export_events(data, aggregated_movements_entity_month_events, group = "aggregated_movements_entity_month_events", include_ers = False, include_annotations = False, include_alerts = True)

    # Aggregated movements per year events
    aggregated_movements_entity_year_events = query.get_events(gauge_names = {"filter": "AGGREGATED_MOVEMENTS_ENTITY_YEAR", "op": "=="},
                                                               stop_filters = [{"date": metadata["reporting_start"], "op": ">"}],
                                                               start_filters = [{"date": metadata["reporting_stop"], "op": "<"}],
                                                               value_filters = [{"name": {"filter": "entity", "op": "=="}, "type": "text", "value": {"op": "==", "filter": metadata["entity"]}}],
                                                               order_by = {"field": "start", "descending": False})
    
    eboa_export.export_events(data, aggregated_movements_entity_year_events, group = "aggregated_movements_entity_year_events", include_ers = False, include_annotations = False, include_alerts = True)

    return render_template("views/transactions_analysis/transactions_analysis.html", data=data)

