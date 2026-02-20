"""
Route Blueprints Registration
"""

def register_blueprints(app):
    """Register all route blueprints with the app"""

    from .health import health_bp
    from .gopro import gopro_bp
    from .recording import recording_bp
    from .games import games_bp
    from .batch import batch_bp
    from .media import media_bp
    from .cloud import cloud_bp
    from .logs import logs_bp
    from .admin import admin_bp
    from .pipeline import pipeline_bp

    # Register blueprints
    app.register_blueprint(health_bp)
    app.register_blueprint(gopro_bp, url_prefix='/api/gopros')
    app.register_blueprint(recording_bp, url_prefix='/api/recording')
    app.register_blueprint(games_bp, url_prefix='/api')
    app.register_blueprint(batch_bp, url_prefix='/api/batch')
    app.register_blueprint(media_bp, url_prefix='/api')
    app.register_blueprint(cloud_bp, url_prefix='/api/cloud')
    app.register_blueprint(logs_bp, url_prefix='/api/logs')
    app.register_blueprint(admin_bp, url_prefix='/api/admin')
    app.register_blueprint(pipeline_bp, url_prefix='/api')
